# ISR-replication flow

## Ideas

- It is fully background process

ISR-module stores in memory:

- replicas list
- in-sync replicas list
- replicas' offsets
- replicas' last fetch time

It can calculate HW out of replicas' offsets

## Sequence

TODO: claude dialogue "Корректность флоу append в Data API"

```mermaid
sequenceDiagram
    box Leader of Partition
        participant Storage
        participant ISR 
    end
    
    box Follower
        participant FISR as Follower ISR
        participant FStorage as Follower Storage
    end

    loop Until configuration is changed
        FISR ->> ISR: Fetch with offset X and maxBytes
        ISR ->> ISR: Resolve in-sync replicas list
        ISR ->> ISR: Resolve HW (High-Watermark) according to in-sync replicas' offset
        ISR ->> Storage: Set HW
        ISR ->> Storage: read(X, maxBytes)
        alt Logs don't exist
            Storage ->> Storage: wait for logs written in file
        end
        Storage ->> Storage: Get logs as usual, but not use HW
        Storage ->> ISR: Returns logs (sendfile structure)
        ISR ->> FISR: Returns logs - sendfile()
        FISR ->> FStorage: Store logs
    end
```

## Kafka reference

```mermaid
sequenceDiagram                                                                                                                                      
      participant ZK as Controller (KRaft)
      participant FM as ReplicaFetcherManager                                                                                                          
      participant FT as ReplicaFetcherThread
      participant LAPI as Leader KafkaApis                                                                                                             
      participant LRM as Leader ReplicaManager                                                                                                         
      participant LP as Leader Partition                                                                                                               
      participant LSEG as Leader LogSegment                                                                                                            
      participant TL as PlaintextTransportLayer                                                                                                        
      participant FRM as Follower ReplicaManager                                                                                                       
      participant FP as Follower Partition                                                                                                             
      participant FSEG as Follower LogSegment                                                                                                          
                                                                                                                                                       
      ZK->>FRM: LeaderAndIsr (назначение лидера)
      FRM->>FM: makeFollower(partition, leaderEpoch)                                                                                                   
      FM->>FT: addPartitions(tp, fetchOffset=LEO)                                                                                                      
      Note over FT: бесконечный loop: doWork()                                                                                                         
                                                                                                                                                       
      loop каждые replica.fetch.wait.max.ms                                                                                                            
          FT->>FT: maybeFetch()                                                                                                                        
          FT->>LAPI: FetchRequest(replicaId=2, offset=LEO, maxBytes=10MB)                                                                              
          Note over FT,LAPI: isFromFollower=true<br/>FetchIsolation.LOG_END (читает uncommitted)                                                       
                                                                                                                                                       
          LAPI->>LRM: fetchMessages(FetchParams(isFromFollower=true))                                                                                  
          LRM->>LP: fetchRecords(fetchPartitionData)                                                                                                   
                                                                                                                                                       
          Note over LP: inReadLock(leaderIsrUpdateLock)
          LP->>LSEG: read(startOffset=LEO, maxSize=10MB)                                                                                               
          Note over LSEG: translateOffset → OffsetIndex.lookup<br/>→ linear scan (pread 13B × N)                                                       
          LSEG-->>LP: FetchDataInfo(FileRecords slice)                                                                                                 
                                                                                                                                                       
          LP->>LP: updateFollowerFetchState(replica, LEO)                                                                                              
          LP->>LP: maybeExpandIsr(followerReplica)                                                                                                     
          alt фолловер догнал лидера (LEO ≥ HW)                                                                                                        
              LP->>LP: maybeIncrementLeaderHW()                                                                                                        
              LP->>ZK: AlterPartition (добавить в ISR)
          end                                                                                                                                          
                  
          LP-->>LRM: LogReadResult                                                                                                                     
          LRM-->>LAPI: Map[partition → FetchDataInfo]
          LAPI->>TL: sendResponse(MultiRecordsSend)                                                                                                    
          TL->>TL: fileChannel.transferTo(pos, len, socket)                                                                                            
          Note over TL: sendfile() - данные идут<br/>page cache → socket, 0 копий                                                                      
                                                                                                                                                       
          TL-->>FT: FetchResponse (bytes по TCP)                                                                                                       
                                                                                                                                                       
          Note over FT: toMemoryRecords()<br/>FileRecords → ByteBuffer.allocate → copy<br/>⚠️  здесь zero-copy заканчивается                            
          FT->>FT: processPartitionData()
          FT->>FP: appendRecordsToFollowerOrFutureReplica(MemoryRecords)                                                                               
          FP->>FSEG: append(records) → write() на диск                                                                                                 
          FSEG->>FSEG: обновить LEO                                                                                                                    
          FSEG-->>FP: LogAppendInfo                                                                                                                    
          FP->>FP: maybeUpdateHighWatermark(leaderHW из ответа)                                                                                        
      end        
```

```mermaid
sequenceDiagram                                                                                                                                      
      participant C as Consumer                                                                                                                        
      participant NET as Network Thread                                                                                                                
      participant RM as ReplicaManager
      participant PG as DelayedOperationPurgatory                                                                                                      
      participant TM as SystemTimer (HashedWheelTimer)
      participant P as Producer (другой клиент)                                                                                                        
      participant AQ as ActionQueue
                                                                                                                                                       
      C->>NET: FetchRequest(offset=LEO, minBytes=1, maxWaitMs=500)                                                                                     
      NET->>RM: fetchMessages(params)                                                                                                                  
      RM->>RM: readFromLog() → 0 байт (нет новых данных)                                                                                               
                                                                                                                                                       
      Note over RM: bytesReadable=0 < minBytes=1<br/>→ нельзя ответить сразу                                                                           
                                                                                                                                                       
      RM->>PG: tryCompleteElseWatch(DelayedFetch, [TopicPartitionKey])                                                                                 
      PG->>PG: safeTryCompleteOrElse()
      PG->>PG: tryComplete() → false (данных нет)                                                                                                      
      PG->>PG: watchForOperation(key, delayedFetch)<br/>добавить в watchersByKey[TopicPartitionKey]                                                    
      PG->>TM: timeoutTimer.add(delayedFetch, delayMs=500ms)                                                                                           
      Note over TM: HashedWheelTimer - запись в bucket<br/>по TTL. Никакого sleep/poll.                                                                
      NET-->>C: (запрос завис, ответа нет)                                                                                                             
      Note over C,TM: ... тишина, пока нет новых данных ...                                                                                            
                                                                                                                                                       
      P->>NET: ProduceRequest(topic, partition, records)                                                                                               
      NET->>RM: appendRecords()                                                                                                                        
      RM->>RM: UnifiedLog.append() → LEO растёт
      RM->>AQ: actionQueue.add { checkAndComplete(key) }                                                                                               
      Note over AQ: ActionQueue - очередь отложенных<br/>действий, выполняется после append                                                            
                                                                                                                                                       
      AQ->>PG: checkAndComplete(TopicPartitionKey)                                                                                                     
      PG->>PG: watchers.tryCompleteWatched()                                                                                                           
      PG->>PG: delayedFetch.tryComplete()                                                                                                              
                                                                                                                                                       
      Note over PG: tryComplete() проверяет:<br/>accumulatedSize >= minBytes? → да
      PG->>PG: forceComplete() → cancel() из таймера                                                                                                   
      PG->>RM: onComplete(): readFromLog(readFromPurgatory=true)
      RM->>RM: читает новые данные из лога                                                                                                             
      RM->>NET: responseCallback(fetchPartitionData)                                                                                                   
      NET-->>C: FetchResponse (с новыми данными)                                                                                                       
                                                                                                                                                       
      alt Данные так и не пришли за 500ms                                                                                                              
          TM->>TM: bucket истёк → TimerTask.run()                                                                                                      
          TM->>PG: delayedFetch.forceComplete()                                                                                                        
          PG->>RM: onComplete(): readFromLog() → пустой ответ                                                                                          
          RM->>NET: responseCallback(empty)                                                                                                            
          NET-->>C: FetchResponse (пустой, consumer делает следующий poll)                                                                             
          PG->>PG: onExpiration() → метрику ExpiresPerSec++                                                                                            
      end               
```