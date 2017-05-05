(ns ophion.proto)

(defn parse-varint [input]
  (letfn [(fold [bytes number input]
            (if (.hasRemaining input)
              (let [b (.get input)
                    done? (zero? (bit-and (long (bit-shift-right b 7)) 0xff))
                    number (.or
                            number
                            (biginteger
                             (bit-shift-left
                              (bit-and b 127)
                              (* bytes 7))))
                    bytes (inc bytes)]
                (if done?
                  number
                  (recur bytes number input)))
              (fn [input]
                (fold bytes number input))))]
    (fold 0 (biginteger 0) input)))


(defn parse-tag [input]
  (letfn [(fold [n k]
            (if (fn? n)
              (fn [input]
                (fold (n input) k))
              (k n)))]
    (fold (parse-varint input)
          (fn [n]
            (let [wire-type (.and n (biginteger 2r111))
                  field-number (.shiftRight n (biginteger 3))]
              {:wire-type wire-type
               :field-tag field-number})))))

(defn parse-fixed-length [len input]
  (letfn [(fold1 [output input]
            (if (.hasRemaining output)
              (if (.hasRemaining input)
                (do
                  (.put output (.get input))
                  (recur output input))
                (fn [input]
                  (fold1 output input)))
              (.flip output)))]
    (fold1 (java.nio.ByteBuffer/allocate len)
           input)))

(defn parse-pair [input]
  (letfn [(fold1 [tag k]
            (if (fn? tag)
              (fn [input]
                (fold1 (tag input) k))
              (k tag)))]
    (fold1 (parse-tag input)
           (fn [tag]
             (case (:wire-type tag)
               0 (fold1 (parse-varint input)
                        (fn [varint]
                          (assoc tag
                                 :type :varint
                                 :value varint)))
               2 (fold1 (parse-varint input)
                        (fn [varint]
                          (fold1 (parse-fixed-length varint input)
                                 (fn [blob]
                                   (assoc tag
                                          :type :bytestring
                                          :value blob))))))))))


(defn parse-n-pairs [n input]
  (letfn [(f [pairs n input]
            (if (zero? n)
              pairs
              (g pairs (dec n) input (parse-pair input))))
          (g [pairs n input x]
            (if (fn? x)
              (fn [input]
                (g pairs n input (x input)))
              (f (conj pairs x)
                 n
                 input)))]
    (f [] n input)))


(parse-n-pairs
 1
 (doto (java.nio.ByteBuffer/allocate 1024)
   (.put (unchecked-byte 0x12))
   (.put (unchecked-byte 0x07))
   (.put (unchecked-byte 0x74))
   (.put (unchecked-byte 0x65))
   (.put (unchecked-byte 0x73))
   (.put (unchecked-byte 0x74))
   (.put (unchecked-byte 0x69))
   (.put (unchecked-byte 0x6e))
   (.put (unchecked-byte 0x67))
   (.flip)))

;;=> [{:wire-type 2, :field-tag 2, :type :bytestring, :value #object[java.nio.HeapByteBuffer 0x340b9973 "java.nio.HeapByteBuffer[pos=0 lim=7 cap=7]"]}]


