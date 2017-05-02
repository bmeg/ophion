(ns ophion.elastic
  (:require
   [clojurewerkz.elastisch.rest :as elastic]
   [clojurewerkz.elastisch.rest.index :as index]))

(defn connect
  [{:keys [host port]}]
  (elastic/connect
   (str
    "http://" (or host "127.0.0.1")
    ":" (or port "9200"))))

