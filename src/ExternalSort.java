import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ExternalSort {
    private static final int TEMP_FILE_SIZE = 1000;
    private static final int THREAD_POOL_SIZE = 4;

    // Class used with multithreading
    private static class ThreadExternalSort implements Callable<String> {

        BufferedReader reader;
        int fileNum;

        public ThreadExternalSort(BufferedReader reader, int fileNum) {
            this.reader = reader;
            this.fileNum = fileNum;
        }

        @Override
        public String call() {
            int[] temp = new int[TEMP_FILE_SIZE];
            int lastIndex = 0;
            for (; lastIndex < TEMP_FILE_SIZE; ++lastIndex) {
                String line = null;
                synchronized (this) {
                    try {
                        line = reader.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (line == null) {
                    if (lastIndex == 0) {
                        return "";
                    }
                    break;
                }
                temp[lastIndex] = Integer.parseInt(line);
            }

            Arrays.sort(temp, 0, lastIndex);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileNum + ".txt"))) {
                for (int i = 0; i < lastIndex; ++i) {
                    writer.append(temp[i] + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return fileNum + ".txt";
        }
    }
    // Perform external sort on nubmers from input file and output to output file with or without
    // multithreading. Time complexity: O(nlog(n)) where n = the size of input file
    public static void externalSort(String input, String output, boolean withThread) {
        try {
            List<String> tempFiles;
            if (withThread) {
                tempFiles = createTempFilesWithThreads(input);
            } else {
                tempFiles = createTempFiles(input);
            }
            BufferedReader[] readers = new BufferedReader[tempFiles.size()];
            for (int i = 0; i < readers.length; ++i) {
                readers[i] = new BufferedReader(new FileReader(tempFiles.get(i)));
            }

            // Create minheap, merge numbers in temp files
            PriorityQueue<int[]> pq = new PriorityQueue<>(readers.length, (a, b) -> a[0] - b[0]); // {num, reader index}
            for (int i = 0; i < readers.length; ++i) {
                pq.add(new int[]{Integer.parseInt(readers[i].readLine()), i});
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
                while (!pq.isEmpty()) {
                    int num = pq.peek()[0];
                    int idx = pq.poll()[1];
                    writer.append(num + "\n");
                    String nextLine = readers[idx].readLine();
                    if (nextLine != null) {
                        pq.add(new int[]{Integer.parseInt(nextLine), idx});
                    }
                }
            }

            // Delete temporary files
            for (String tempFile : tempFiles) {
                new File(tempFile).delete();
            }
        } catch (IOException e) {
            System.out.println("Cannot perform external sort");
            e.printStackTrace();
        }
    }

    // Split the input file into arrays of size TEMP_FILE_SIZE, sort and store them in temporary
    // files
    private static List<String> createTempFiles(String fileName) {
        List<String> tempFiles = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line = reader.readLine();
            int fileNum = 0;

            while (line != null) {
                int[] temp = new int[TEMP_FILE_SIZE];

                int lastIndex = 0;
                for (; lastIndex < TEMP_FILE_SIZE; ++lastIndex) {
                    if (line == null) {
                        break;
                    }
                    temp[lastIndex] = Integer.parseInt(line);
                    line = reader.readLine();
                }

                Arrays.sort(temp, 0, lastIndex);

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileNum + ".txt"))) {
                    for (int i = 0; i < lastIndex; ++i) {
                        writer.append(temp[i] + "\n");
                    }
                }
                tempFiles.add(fileNum++ + ".txt");
            }
        } catch (IOException e) {
            System.out.println("Cannot create temporary files");
        }
        return tempFiles;
    }

    // Split the input file into arrays of size TEMP_FILE_SIZE, sort and store them in temporary
    // files with multithreading
    public static List<String> createTempFilesWithThreads(String fileName) {
        List<String> tempFiles = new ArrayList<>();
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            int fileNum = 0;
            List<Future<String>> futures;
            boolean hasNext = true;

            while (hasNext) {
                futures = new ArrayList<>();
                for (int i = 0; i < THREAD_POOL_SIZE; ++i) {
                    Future<String> future = executor
                            .submit(new ThreadExternalSort(reader, fileNum));
                    futures.add(future);
                    fileNum++;
                }

                try {
                    for (Future<String> future : futures) {
                        String tempFile = future.get();
                        if (tempFile.equals("")) {
                            hasNext = false;
                        } else {
                            tempFiles.add(tempFile);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Join the threads
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("Executor termination interrupted");
        } finally {
            executor.shutdownNow();
        }
        return tempFiles;
    }
    // Generate random input file
    private static void generateInput(String fileName, long size) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            Random rand = new Random();
            for (int i = 0; i < size; ++i) {
                writer.append(rand.nextInt() + "\n");
            }
        } catch (IOException e) {
            System.out.println("Cannot generate input file");
        }
    }
    public static void main(String[] args) {
        String input = "input.txt";
        generateInput(input, 10000);
        externalSort(input, "output.txt", false);
        externalSort(input, "output_threads.txt", true);
   }
}
