package streaming.input;

/**
 * 1、readTextFile(path) - Reads text files, i.e. files that respect the TextInputFormat specification, line-by-line and returns them as Strings.
 *
 * 2、readFile(fileInputFormat, path) - Reads (once) files as dictated by the specified file input format.
 *
 * 3、readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo) - This is the method called internally by the two previous ones. It reads files in the path based on the given fileInputFormat. Depending on the provided watchType, this source may periodically monitor (every interval ms) the path for new data (FileProcessingMode.PROCESS_CONTINUOUSLY), or process once the data currently in the path and exit (FileProcessingMode.PROCESS_ONCE). Using the pathFilter, the user can further exclude files from being processed.
 *
 * IMPLEMENTATION:
 *
 * Under the hood, Flink splits the file reading process into two sub-tasks, namely directory monitoring and data reading. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a single, non-parallel (parallelism = 1) task, while reading is performed by multiple tasks running in parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring task is to scan the directory (periodically or only once depending on the watchType), find the files to be processed, divide them in splits, and assign these splits to the downstream readers. The readers are the ones who will read the actual data. Each split is read by only one reader, while a reader can read multiple splits, one-by-one.
 *
 * IMPORTANT NOTES:
 *
 * If the watchType is set to FileProcessingMode.PROCESS_CONTINUOUSLY, when a file is modified, its contents are re-processed entirely. This can break the “exactly-once” semantics, as appending data at the end of a file will lead to all its contents being re-processed.
 *
 * If the watchType is set to FileProcessingMode.PROCESS_ONCE, the source scans the path once and exits, without waiting for the readers to finish reading the file contents. Of course the readers will continue reading until all file contents are read. Closing the source leads to no more checkpoints after that point. This may lead to slower recovery after a node failure, as the job will resume reading from the last checkpoint.
 *
 * Socket-based:
 *
 * socketTextStream - Reads from a socket. Elements can be separated by a delimiter.
 * Collection-based:
 *
 * fromCollection(Collection) - Creates a data stream from the Java Java.util.Collection. All elements in the collection must be of the same type.
 *
 * fromCollection(Iterator, Class) - Creates a data stream from an iterator. The class specifies the data type of the elements returned by the iterator.
 *
 * fromElements(T ...) - Creates a data stream from the given sequence of objects. All objects must be of the same type.
 *
 * fromParallelCollection(SplittableIterator, Class) - Creates a data stream from an iterator, in parallel. The class specifies the data type of the elements returned by the iterator.
 *
 * generateSequence(from, to) - Generates the sequence of numbers in the given interval, in parallel.
 *
 * Custom:
 *
 * addSource - Attach a new source function. For example, to read from Apache Kafka you can use addSource(new FlinkKafkaConsumer08<>(...)). See connectors for more details.
 */
