package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)


	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var waitGroup sync.WaitGroup
	//waitGroup.Add(ntasks)

	for taskNumber:=0; taskNumber<ntasks ; taskNumber++  {
		waitGroup.Add(1)
		go func (taskNumber int, nios int, phase jobPhase){
			defer waitGroup.Done()
			//var result bool = false
			for{
					debug("task: %v", taskNumber)
					var workerName = <-mr.registerChannel
					var doTaskArgs = DoTaskArgs{
						JobName:       mr.jobName,
						File:          mr.files[taskNumber],
						Phase:         phase,
						TaskNumber:    taskNumber,
						NumOtherPhase: nios,
					}


					var reply struct{}
					result := call(workerName, "Worker.DoTask", doTaskArgs, &reply)
					if result {
						go func (){
							mr.registerChannel <- workerName
						}()
						break
					}
			}

		} (taskNumber, nios, phase)
	}

	waitGroup.Wait()


	fmt.Printf("Schedule: %v phase done\n", phase)
}




