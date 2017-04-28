(ns ophion.config
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]))

(defn resource
  [path]
  (-> path
      io/resource
      io/input-stream
      io/reader))

(defn read-config
  [path]
  (edn/read
   (java.io.PushbackReader.
    (resource path))))
