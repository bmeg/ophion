(ns ophion.elastic
  (:require
   [clojurewerkz.elastisch.rest :as esr]))

(defn connect
  [{:keys [host port]}]
  (esr/connect
   (str
    "http://" (or host "127.0.0.1")
    ":" (or port "9200"))))

