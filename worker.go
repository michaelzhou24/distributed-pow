package distpow

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

/****** Tracer structs ******/
type WorkerConfig struct {
	WorkerID         string
	ListenAddr       string
	CoordAddr        string
	TracerServerAddr string
	TracerSecret     []byte
}

type WorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type WorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type WorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

/****** RPC structs ******/
type WorkerResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	TraceToken       tracing.TracingToken
}

type WorkerMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	WorkerBits       uint
	TraceToken       tracing.TracingToken
}

type WorkerCancelArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	TraceToken       tracing.TracingToken
}

type CancelChan chan struct{}

type Worker struct {
	config        WorkerConfig
	Tracer        *tracing.Tracer
	Coordinator   *rpc.Client
	mineTasks     map[string]CancelChan
	ResultChannel chan WorkerResultArgs
}

type WorkerMineTasks struct {
	mu    sync.Mutex
	tasks map[string]CancelChan
}

type WorkerRPCHandler struct {
	tracer      *tracing.Tracer
	coordinator *rpc.Client
	mineTasks   WorkerMineTasks
	resultChan  chan WorkerResultArgs
	mu          sync.Mutex
	cache       map[string][]uint8
	nonceMap    map[string]uint
}

func NewWorker(config WorkerConfig) *Worker {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.WorkerID,
		Secret:         config.TracerSecret,
	})

	coordClient, err := rpc.Dial("tcp", config.CoordAddr)
	if err != nil {
		log.Fatal("failed to dail Coordinator:", err)
	}

	return &Worker{
		config:        config,
		Tracer:        tracer,
		Coordinator:   coordClient,
		mineTasks:     make(map[string]CancelChan),
		ResultChannel: make(chan WorkerResultArgs),
	}
}

func (w *Worker) InitializeWorkerRPCs() error {
	server := rpc.NewServer()
	err := server.Register(&WorkerRPCHandler{
		tracer:      w.Tracer,
		coordinator: w.Coordinator,
		mineTasks: WorkerMineTasks{
			tasks: make(map[string]CancelChan),
		},
		resultChan: w.ResultChannel,
		cache:      make(map[string][]uint8),
		nonceMap:   make(map[string]uint),
	})

	// publish Worker RPCs
	if err != nil {
		return fmt.Errorf("format of Worker RPCs aren't correct: %s", err)
	}

	listener, e := net.Listen("tcp", w.config.ListenAddr)
	if e != nil {
		return fmt.Errorf("%s listen error: %s", w.config.WorkerID, e)
	}

	log.Printf("Serving %s RPCs on port %s", w.config.WorkerID, w.config.ListenAddr)
	go server.Accept(listener)

	return nil
}

// Mine is a non-blocking async RPC from the Coordinator
// instructing the worker to solve a specific pow instance.
func (w *WorkerRPCHandler) Mine(args WorkerMineArgs, reply *RPCToken) error {
	trace := w.tracer.ReceiveToken(args.TraceToken)
	// add new task
	cancelCh := make(chan struct{}, 1)
	w.mineTasks.set(args.Nonce, args.NumTrailingZeros, args.WorkerByte, cancelCh)

	trace.RecordAction(WorkerMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
		WorkerByte:       args.WorkerByte,
	})
	if val := w.cacheContains(args.Nonce, args.NumTrailingZeros); val != nil {
		trace.RecordAction(CacheHit{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			Secret:           val,
		})
		trace.RecordAction(WorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           val,
		})
		result := WorkerResultArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           val,
			TraceToken:       trace.GenerateToken(),
		}
		w.resultChan <- result
		<-cancelCh
		// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
		trace.RecordAction(WorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})
		// ACK the cancellation; the coordinator will be waiting for this.
		w.resultChan <- WorkerResultArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           nil,
			TraceToken:       trace.GenerateToken(),
		}
		reply.TraceToken = trace.GenerateToken()
		return nil
	} else {
		trace.RecordAction(CacheMiss{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
		})
		go miner(trace, w, args, cancelCh)
		reply.TraceToken = trace.GenerateToken()
		return nil
	}
}

// Cancel is a non-blocking async RPC from the Coordinator
// instructing the worker to stop solving a specific pow instance.
// Update the cache also
func (w *WorkerRPCHandler) Found(args WorkerCancelArgs, reply *RPCToken) error {
	trace := w.tracer.ReceiveToken(args.TraceToken)
	if val := w.cacheContains(args.Nonce, args.NumTrailingZeros); val != nil { // more zeroes or equivalent
		trace.RecordAction(CacheHit{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			Secret:           val,
		})
	} else {
		trace.RecordAction(CacheMiss{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
		})
	}

	w.updateCache(trace, args.NumTrailingZeros, w.cache, w.nonceMap, args.Nonce, args.Secret)
	cancelChan, ok := w.mineTasks.get(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	if !ok {
		log.Printf("Received more than once cancellation for %s\n", generateWorkerTaskKey(args.Nonce, args.NumTrailingZeros, args.WorkerByte))
		// ACK the found result; the coordinator will be waiting for this.
		trace.RecordAction(WorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})
		w.resultChan <- WorkerResultArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           nil,
			TraceToken:       trace.GenerateToken(),
		}
		reply.TraceToken = trace.GenerateToken()
		return nil
	}
	cancelChan <- struct{}{}
	// delete the task here, and the worker should terminate + send something back very soon
	w.mineTasks.delete(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	reply.TraceToken = trace.GenerateToken()
	return nil
}

// nil if not in cache
// return secret in cache otherwise
func (w *WorkerRPCHandler) cacheContains(nonce []uint8, numTrailingZeroes uint) []uint8 {
	w.mu.Lock()
	defer w.mu.Unlock()
	cacheKey := byteSliceToString(nonce)
	if val, ok := w.cache[cacheKey]; ok {
		if t, ok2 := w.nonceMap[cacheKey]; ok2 && t >= numTrailingZeroes {
			return val
		}
	}
	return nil
}

/*
- Update the cache when the a worker sends a result back to the coordinator.
- Remove cache entry with (n1, t) if an entry (n1, t+1) is added.
*/
func (w *WorkerRPCHandler) updateCache(trace *tracing.Trace, numTrailingZeroes uint, cache map[string][]uint8, nonceMap map[string]uint, nonce []uint8, secret []uint8) {
	cacheKey := byteSliceToString(nonce)
	//log.Printf("Secret given: %x", secret)
	//trailingZeroes := getNumTrailingZeroes(nonce, secret)
	w.mu.Lock()
	defer w.mu.Unlock()
	if val, ok := cache[cacheKey]; ok {
		if compare(nonce, numTrailingZeroes, nonceMap, val, secret) {
			trace.RecordAction(CacheRemove{
				Nonce:            nonce,
				NumTrailingZeros: nonceMap[byteSliceToString(nonce)],
				Secret:           cache[cacheKey],
			})
			trace.RecordAction(CacheAdd{
				Nonce:            nonce,
				NumTrailingZeros: numTrailingZeroes,
				Secret:           secret,
			})
			nonceMap[cacheKey] = numTrailingZeroes
			cache[cacheKey] = secret
		}
	} else {
		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeroes,
			Secret:           secret,
		})
		nonceMap[cacheKey] = numTrailingZeroes
		cache[cacheKey] = secret
	}
	fmt.Println("Cache state: ", cache)
	fmt.Println(nonceMap)
}

func nextChunk(chunk []uint8) []uint8 {
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == 0xFF {
			chunk[i] = 0
		} else {
			chunk[i]++
			return chunk
		}
	}
	return append(chunk, 1)
}

func hasNumZeroesSuffix(str []byte, numZeroes uint) bool {
	var trailingZeroesFound uint
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '0' {
			trailingZeroesFound++
		} else {
			break
		}
	}
	return trailingZeroesFound >= numZeroes
}

func miner(trace *tracing.Trace, w *WorkerRPCHandler, args WorkerMineArgs, killChan <-chan struct{}) {
	chunk := []uint8{}
	remainderBits := 8 - (args.WorkerBits % 9)

	hashStrBuf, wholeBuffer := new(bytes.Buffer), new(bytes.Buffer)
	if _, err := wholeBuffer.Write(args.Nonce); err != nil {
		panic(err)
	}
	wholeBufferTrunc := wholeBuffer.Len()

	// table out all possible "thread bytes", aka the byte prefix
	// between the nonce and the bytes explored by this worker
	remainderEnd := 1 << remainderBits
	threadBytes := make([]uint8, remainderEnd)
	for i := 0; i < remainderEnd; i++ {
		threadBytes[i] = uint8((int(args.WorkerByte) << remainderBits) | i)
	}

	for {
		for _, threadByte := range threadBytes {
			select {
			case <-killChan:
				trace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				w.resultChan <- WorkerResultArgs{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil, // nil secret treated as cancel completion
					TraceToken:       trace.GenerateToken(),
				}
				return
			default:
				// pass
			}
			wholeBuffer.Truncate(wholeBufferTrunc)
			if err := wholeBuffer.WriteByte(threadByte); err != nil {
				panic(err)
			}
			if _, err := wholeBuffer.Write(chunk); err != nil {
				panic(err)
			}
			hash := md5.Sum(wholeBuffer.Bytes())
			hashStrBuf.Reset()
			fmt.Fprintf(hashStrBuf, "%x", hash)
			if hasNumZeroesSuffix(hashStrBuf.Bytes(), args.NumTrailingZeros) {
				trace.RecordAction(WorkerResult{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
				})
				result := WorkerResultArgs{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
					TraceToken:       trace.GenerateToken(),
				}
				w.resultChan <- result
				// Update local cache immediately upon finding
				w.updateCache(trace, result.NumTrailingZeros, w.cache, w.nonceMap, result.Nonce, result.Secret)
				// now, wait for the worker the receive a cancellation,
				// which the coordinator should always send no matter what.
				// note: this position takes care of interleavings where cancellation comes after we check killChan but
				//       before we log the result we found, forcing WorkerCancel to be the last action logged, even in that case.
				<-killChan
				// @429
				// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
				trace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				// ACK the cancellation; the coordinator will be waiting for this.
				w.resultChan <- WorkerResultArgs{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil,
					TraceToken:       trace.GenerateToken(),
				}

				return
			}
		}
		chunk = nextChunk(chunk)
	}
}

func (t *WorkerMineTasks) get(nonce []uint8, numTrailingZeros uint, workerByte uint8) (CancelChan, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)]
	return t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)], ok
}

func (t *WorkerMineTasks) set(nonce []uint8, numTrailingZeros uint, workerByte uint8, val CancelChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)] = val
}

func (t *WorkerMineTasks) delete(nonce []uint8, numTrailingZeros uint, workerByte uint8) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateWorkerTaskKey(nonce, numTrailingZeros, workerByte))
}

func generateWorkerTaskKey(nonce []uint8, numTrailingZeros uint, workerByte uint8) string {
	return fmt.Sprintf("%s|%d|%d", hex.EncodeToString(nonce), numTrailingZeros, workerByte)
}
