(ns jepsen.store.file-offset-output-stream-test
  (:require [clojure.test :refer :all]
            [jepsen.store.format :as format])
  (:import (java.io File IOException RandomAccessFile)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel)
           (java.nio.file Files)
           (java.util.zip CRC32)
           (jepsen.store.format FileOffsetOutputStream)))

(defn with-temp-file [f]
  (let [file (File/createTempFile "jepsen-output-" ".bin")]
    (try
      (with-open [raf (RandomAccessFile. file "rw")]
        (f file (.getChannel raf)))
      (finally (.delete file)))))

(defn limited-channel
  "Keep actual file writes, limiting only the byte count returned by a call."
  [^FileChannel file allowance calls]
  (proxy [FileChannel] []
    (write [^ByteBuffer buf position]
      (let [limit (.limit buf)
            n (swap! calls inc)]
        (try
          (.limit buf (+ (.position buf)
                         (min (.remaining buf) (long (allowance n)))))
          (.write file buf (long position))
          (finally (.limit buf limit)))))
    (read [^ByteBuffer buf position] (.read file buf (long position)))
    (size [] (.size file))
    (force [metadata?] (.force file metadata?))
    (implCloseChannel [])))

(defn crc [bytes]
  (let [checksum (CRC32.)]
    (.update checksum (byte-array bytes))
    (.getValue checksum)))

(deftest complete-writes-retain-content-offset-and-checksum
  (with-temp-file
    (fn [file channel]
      (let [stream (FileOffsetOutputStream. channel 3 (CRC32.))
            bytes (byte-array [9 6 7 8])]
        (.write stream (byte-array [1 2 3 4 5]))
        (.write stream bytes 1 2)
        (.write stream bytes 4 0)
        (.write stream (int 264))
        (is (= [0 0 0 1 2 3 4 5 6 7 8]
               (vec (Files/readAllBytes (.toPath file)))))
        (is (= 8 (.bytesWritten stream)))
        (is (= (crc [1 2 3 4 5 6 7 8])
               (.getValue (.checksum stream))))))))

(deftest incomplete-writes-throw-without-retrying
  (doseq [[name write! allowance]
          [["single byte, zero" #(.write ^FileOffsetOutputStream % (int 7)) 0]
           ["array, short" #(.write ^FileOffsetOutputStream % (byte-array [1 2 3])) 2]
           ["array, zero" #(.write ^FileOffsetOutputStream % (byte-array [1 2 3])) 0]
           ["slice, short" #(.write ^FileOffsetOutputStream % (byte-array [9 1 2 3]) 1 3) 2]
           ["slice, zero" #(.write ^FileOffsetOutputStream % (byte-array [9 1 2 3]) 1 3) 0]]]
    (testing name
      (with-temp-file
        (fn [_ channel]
          (let [calls (atom 0)
                stream (FileOffsetOutputStream.
                         (limited-channel channel (constantly allowance) calls)
                         0 (CRC32.))]
            (is (thrown? IOException (write! stream)))
            (is (= 1 @calls))))))))

(deftest incomplete-payload-is-not-registered-as-a-fressian-block
  (with-temp-file
    (fn [file _]
      (with-open [handle (format/open file)]
        (format/prep-write! handle)
        (let [id (format/new-block-id! handle)
              before @(:block-index handle)
              channel (limited-channel (:file handle)
                                       #(if (= 1 %) 2 Integer/MAX_VALUE)
                                       (atom 0))]
          (is (thrown? IOException
                       (format/write-fressian-block!
                         (assoc handle :file channel) id {:payload [1 2 3 4 5]})))
          (is (= before @(:block-index handle))))))))
