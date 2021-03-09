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
	Secret           []uint8
}

/****** RPC structs ******/
type WorkerMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	WorkerBits       uint
	TraceToken tracing.TracingToken
}

type WorkerCancelArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	TraceToken tracing.TracingToken
}

type CancelChan chan struct{}

type Worker struct {
	config        WorkerConfig
	Tracer        *tracing.Tracer
	Coordinator   *rpc.Client
	mineTasks     map[string]CancelChan
	ResultChannel chan WorkerResult
}

type WorkerMineTasks struct {
	mu    sync.Mutex
	tasks map[string]CancelChan
}

type WorkerRPCHandler struct {
	tracer      *tracing.Tracer
	coordinator *rpc.Client
	mineTasks   WorkerMineTasks
	resultChan  chan WorkerResult
	cache map[string][]uint8
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
		ResultChannel: make(chan WorkerResult),
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
		cache: make(map[string][]uint8),
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
func (w *WorkerRPCHandler) Mine(args WorkerMineArgs, reply *struct{}) error {
	trace := w.tracer.ReceiveToken(args.TraceToken)
	// add new task
	cancelCh := make(chan struct{}, 1)
	w.mineTasks.set(args.Nonce, args.NumTrailingZeros, args.WorkerByte, cancelCh)

	trace.RecordAction(WorkerMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
		WorkerByte:       args.WorkerByte,
	})
	go miner(w, args, cancelCh)

	return nil
}

// Cancel is a non-blocking async RPC from the Coordinator
// instructing the worker to stop solving a specific pow instance.
// Update the cache also
func (w *WorkerRPCHandler) Found(args WorkerCancelArgs, reply *struct{}) error {
	trace := w.tracer.ReceiveToken(args.TraceToken)
	w.updateCache(trace, args.Nonce, args.NumTrailingZeros, args.Secret)
	cancelChan, ok := w.mineTasks.get(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	if !ok {
		log.Printf("Received more than once cancellation for %s\n", generateWorkerTaskKey(args.Nonce, args.NumTrailingZeros, args.WorkerByte))
		// ACK the found result; the coordinator will be waiting for this.
		w.resultChan <- WorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           nil,
		}
		return nil
	}
	cancelChan <- struct{}{}
	// delete the task here, and the worker should terminate + send something back very soon
	w.mineTasks.delete(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	return nil
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

func miner(w *WorkerRPCHandler, args WorkerMineArgs, killChan <-chan struct{}) {
	trace := w.tracer.ReceiveToken(args.TraceToken)
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
				w.resultChan <- WorkerResult{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil, // nil secret treated as cancel completion
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
				result := WorkerResult{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
				}
				trace.RecordAction(result)
				w.resultChan <- result
				// Update local cache immediately upon finding
				w.updateCache(trace, result.Nonce, result.NumTrailingZeros, result.Secret)
				// now, wait for the worker the receive a cancellation,
				// which the coordinator should always send no matter what.
				// note: this position takes care of interleavings where cancellation comes after we check killChan but
				//       before we log the result we found, forcing WorkerCancel to be the last action logged, even in that case.
				<-killChan

				// ACK the cancellation; the coordinator will be waiting for this.
				w.resultChan <- WorkerResult{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil,
				}
				// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
				trace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				return
			}
		}
		chunk = nextChunk(chunk)
	}
}

func (w *WorkerRPCHandler) updateCache(trace *tracing.Trace, nonce []uint8, numTrailingZeroes uint, secret []uint8) {
	// TODO : check if secret dominates found secret

	cacheKey := byteSliceToString(nonce)
	log.Printf("Secret given: %x", secret)
	trailingZeroes := getNumTrailingZeroes(nonce, secret)
	if val, ok := w.cache[cacheKey]; ok {
		if trailingZeroes > getNumTrailingZeroes(nonce, val) {
			trace.RecordAction(CacheAdd{
				Nonce:            nonce,
				NumTrailingZeros: numTrailingZeroes,
				Secret:           secret,
			})
			w.cache[cacheKey] = secret
		}
	} else {
		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeroes,
			Secret:           secret,
		})
		w.cache[cacheKey] = secret
	}
	fmt.Println("worker cache state: " , w.cache)
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
