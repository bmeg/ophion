(ns ophion.kafka
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojurewerkz.propertied.properties :as props])
  (:import
   [java.util Properties UUID]
   [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
   [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]))

(def string-serializer
  {"key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(def string-deserializer
  {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(def consumer-defaults
  (merge
   string-deserializer
   {"auto.offset.reset" "earliest"
    "enable.auto.commit" "true"
    "auto.commit.interval.ms" "1000"
    "request.timeout.ms" "140000"
    "session.timeout.ms" "120000"}))

(defn uuid
  []
  (str (UUID/randomUUID)))

(defn producer
  ([host] (producer host {}))
  ([host props]
   (let [config (merge string-serializer {"bootstrap.servers" host} props)]
     (new KafkaProducer config))))

(defn send
  [producer topic message]
  (let [record (new ProducerRecord topic (uuid) message)]
    (.send producer record)))

(defn consumer
  ([host group-id topics] (consumer host group-id topics {}))
  ([host group-id topics props]
   (let [config (merge
                 consumer-defaults
                 {"bootstrap.servers" host
                  "group.id" group-id}
                 props)
         devour (new KafkaConsumer (props/map->properties config))]
     (if-not (empty? topics)
       (.subscribe devour topics))
     devour)))

(defn consume
  [consumer handle-message]
  (while true
    (let [records (.poll consumer 10000)]
      (for [record records]
        (handle-message record)))))

(defn list-topics
  [consumer]
  (let [topic-map (into {} (.listTopics consumer))]
    (into
     {}
     (map
      (fn [[k t]]
        [k (into [] t)])
      topic-map))))

(defn path->topic
  [path]
  (let [parts (string/split path #"\.")]
    (string/join "." (butlast parts))))

(defn path->label
  [path]
  (let [parts (string/split path #"\.")]
    (-> parts reverse (drop 1) first)))

(defn file->topic
  [producer file topic]
  (for [line (line-seq (io/reader file))]
    (send producer topic line)))

(defn dir->topics
  [producer path]
  (let [files (filter #(.isFile %) (file-seq path))]
    (for [file files]
      (let [topic (path->topic (.getName file))]
        (file->topic producer file topic)))))
