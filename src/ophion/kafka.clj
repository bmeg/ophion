(ns ophion.kafka
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [taoensso.timbre :as log]
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
  ([host group-id] (consumer host group-id []))
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
      (doseq [record records]
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
  "removes the suffix at the end of a path"
  (let [parts (string/split path #"\.")]
    (string/join "." (butlast parts))))

(defn path->label
  [path]
  "extracts the penultimate element out of a path"
  (let [parts (string/split path #"\.")]
    (-> parts reverse (drop 1) first)))

(defn topic->label
  [topic]
  "returns the suffix at the end of a path"
  (let [parts (string/split topic #"\.")]
    (last parts)))

(defn file->stream
  [out file topic]
  (doseq [line (line-seq (io/reader file))]
    (send out topic line)))

(defn dir->streams
  [out path]
  (let [files (filter #(.isFile %) (file-seq (io/file path)))]
    (doseq [file files]
      (let [topic (path->topic (.getName file))]
        (log/info "populating new topic" topic)
        (file->stream out file topic)))))

(defn spout-dir
  [config path]
  (let [host (:host config)
        spout (producer host)]
    (dir->streams spout path)))

(def default-config
  {:host (or (System/getenv "KAFKA_HOST") "localhost:9092")
   :consumer
   {:group-id (uuid)}})

(defn -main
  [& args]
  (doseq [path args]
    (log/info "spouting" path)
    (spout-dir default-config path)))
