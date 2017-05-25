(ns ophion.config
  (:require
   [clojure.edn :as edn]
   [clojure.string :as string]
   [clojure.java.io :as io]))

(defn resource
  [path]
  (-> path
      io/resource
      io/input-stream
      io/reader))

(defn read-resource
  [path]
  (let [to (resource path)
        in (string/join "\n" (line-seq to))]
    (.close to)
    in))

(defn read-config
  [path]
  (edn/read
   (java.io.PushbackReader.
    (resource path))))
