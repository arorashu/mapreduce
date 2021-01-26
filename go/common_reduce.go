package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).

	var inFilePointers = make([]*os.File, nMap)
	//var jsonDecoders = make([]*json.Decoder, nMap)
	var keyToValues = make(map[string][]string)


	for i := 0; i < nMap; i++ {
		var inFileName = reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(inFileName)
		checkError(err, "Open")
		inFilePointers[i] = f
		decoder := json.NewDecoder(f)

		for decoder.More() {
			var kv KeyValue
			// decode an array value (KeyValue)
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			//
			//if keyToValues[kv.Key] == nil {
			//	keyToValues[kv.Key] = [kv.Value]
			//}
			keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)

			//fmt.Printf("%v: %v\n", kv.Key, kv.Value)

		}

	}

	var outFileName =  mergeName(jobName, reduceTaskNumber)
	f, err := os.Create(outFileName)
	checkError(err, "file create")
	enc := json.NewEncoder(f)

	for key, value := range keyToValues {
		err = enc.Encode(KeyValue{key, reduceF(key, value)})
	}

	for _, f := range inFilePointers {
		err = f.Close()
		checkError(err, "file close")
	}


	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
