(ns ophion.analysis
  (:require
   [clojure.set :as set]
   [ophion.aggregate :as aggregate]))

(def gods-graph
  {:vertexes
   [{:label "location" :gid "sky" :data {:name "sky"}}
    {:label "location" :gid "sea" :data {:name "sea"}}
    {:label "location" :gid "tartarus" :data {:name "tartarus"}}
    {:label "titan" :gid "saturn" :data {:name "saturn" :age 5000}}
    {:label "titan" :gid "saturn" :data {:name "saturn" :age 10000}}
    {:label "god" :gid "jupiter" :data {:name "jupiter" :age 5000}}
    {:label "god" :gid "neptune" :data {:name "neptune" :age 4500}}
    {:label "god" :gid "pluto" :data {:name "pluto"}}
    {:label "god" :gid "pluto" :data {:age 4000}}
    {:label "demigod" :gid "hercules" :data {:name "hercules" :age 30}}
    {:label "human" :gid "alcmene" :data {:name "alcmene" :age 45}}
    {:label "monster" :gid "nemean" :data {:name "nemean"}}
    {:label "monster" :gid "hydra" :data {:name "hydra"}}
    {:label "monster" :gid "cerberus" :data {:name "cerberus"}}]
   :edges
   [{:fromLabel "god" :from "jupiter" :label "father" :toLabel "god" :to "saturn"}
    {:fromLabel "god" :from "jupiter" :label "brother" :toLabel "god" :to "neptune"}
    {:fromLabel "god" :from "jupiter" :label "brother" :toLabel "god" :to "neptune"}
    {:fromLabel "god" :from "jupiter" :label "brother" :toLabel "god" :to "pluto"}
    {:fromLabel "god" :from "jupiter" :label "lives" :toLabel "location" :to "sky" :data {:reason "likes wind" :much 0.3}}
    {:fromLabel "god" :from "neptune" :label "brother" :toLabel "god" :to "jupiter"}
    {:fromLabel "god" :from "neptune" :label "brother" :toLabel "god" :to "pluto"}
    {:fromLabel "god" :from "neptune" :label "lives" :toLabel "location" :to "sea" :data {:reason "likes waves" :much 0.4}}
    {:fromLabel "god" :from "pluto" :label "brother" :toLabel "god" :to "jupiter"}
    {:fromLabel "god" :from "pluto" :label "brother" :toLabel "god" :to "neptune"}
    {:fromLabel "god" :from "pluto" :label "lives" :toLabel "location" :to "tartarus" :data {:reason "likes death" :much 0.5}}
    {:fromLabel "demigod" :from "hercules" :label "father" :toLabel "god" :to "jupiter"}
    {:fromLabel "demigod" :from "hercules" :label "mother" :toLabel "human" :to "alcmene"}
    {:fromLabel "demigod" :from "hercules" :label "battled" :toLabel "monster" :to "nemean" :data {:trial 1}}
    {:fromLabel "demigod" :from "hercules" :label "battled" :toLabel "monster" :to "hydra" :data {:trial 2}}
    {:fromLabel "monster" :from "hercules" :label "battled" :toLabel "monster" :to "cerberus" :data {:trial 12}}
    {:fromLabel "monster" :from "cerberus" :label "lives" :toLabel "location" :to "tartarus"}]})










;; count: 2958
;; time: 628.886025 msecs

(defn compounds
  []
  (time
   (aggregate/evaluate
    db
    "Individual"
    [[:mark "individual"]
     [:to "drugTherapyFrom" "Compound"]
     [:mark "compound"]
     [:select ["individual" "compound"]]])))



;; count: 24690
;; time: 70984.33431 msecs

(defn targets
  []
  (time
   (aggregate/evaluate
    db
    "Compound"
    [[:mark "compound"]
     [:from "environmentFor" "G2PAssociation"]
     [:toUnique "genotypeOf" "Gene"]
     [:mark "gene"]
     [:select ["gene" "compound"]]])))



;; count: 1544751
;; time: 493228.59777 msecs

(defn variants
  []
  (time
   (aggregate/evaluate
    db
    "Individual"
    [[:mark "individual"]
     [:from "sampleOf" "Biosample"]
     [:from "callSetOf" "CallSet" {"vertex.method" "MUTECT"}]
     [:fromUnique "variantOf" "Variant"]
     [:toUnique "variantIn" "Gene"]
     [:mark "gene"]
     [:select ["individual" "gene"]]])))


(defn group-with
  [by with initial q]
  (reduce
   (fn [groups thing]
     (let [key (by thing)
           path (if (coll? key) key [key])
           value (with thing)]
       (update-in groups path (fnil conj #{}) value)))
   initial q))

(defn variant-map
  [initial variants]
  (group-with
   (fn [variant] [(get variant "individual") "variants"])
   (fn [variant] (get variant "gene"))
   initial variants))

(defn compound-map
  [initial compounds]
  (group-with
   (fn [compound] [(get compound "individual") "compounds"])
   (fn [compound] (get compound "compound"))
   initial compounds))

(defn target-map
  [initial targets]
  (group-with
   (fn [target] (get target "gene"))
   (fn [target] (get target "compound"))
   initial targets))

(defn individual-map
  [variants compounds]
  (-> {}
      (variant-map variants)
      (compound-map compound)))

(defn evaluate-individuals
  [variants compounds targets]
  (let [individuals (individual-map variants compounds)
        associations (target-map {} targets)]
    (map
     (fn [[part individual]]
       (let [genes (get individual "variants")]
         (if (empty? genes)
           {:unsupported (count (get individual "compounds"))}
           (let [implied (apply set/union (map associations (get individual "variants")))
                 applied (get individual "compounds")]
             {:supported (count (set/intersection applied implied))
              :unsupported (count (set/difference applied implied))
              :potential (count (set/difference implied applied))}))))
     individuals)))

















;; [{:$match {:label "Biosample"}}
;;  {:$lookup {:from "edge", :localField "gid", :foreignField "to", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {:to.label "hasSample"}}
;;  {:$addFields {"to._history" "$history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$lookup {:from "vertex", :localField "from", :foreignField "gid", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {}}
;;  {:$addFields {"to._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$group {:_id "$name", :count {:$sum 1}}}
;;  {:$project {:key "$_id", :count "$count"}}]



;; [[:fromUnique "callSetOf" "CallSet"][:fromUnique "variantOf" "Variant"] [:toUnique "featureOf" "G2PAssociation"] [:toUnique "environmentFor" "Compound"] [:from "responseTo" "ResponseCurve"] [:toUnique "responseFor" "Biosample"] [:count]]





;; (def possibles
;;   (time
;;    (aggregate/evaluate
;;     db
;;     "Individual"
;;     [[:mark "individual"]
;;      [:from "sampleOf" "Biosample"]
;;      [:from "callSetOf" "CallSet" {"vertex.method" "MUTECT"}]
;;      [:from "variantOf" "Variant"]
;;      [:to "variantIn" "Gene"]
;;      [:mark "gene"]
;;      [:select ["individual" "gene"]]])))





;; (time
;;  (aggregate/evaluate
;;   db
;;   "G2PAssociation"
;;   [[:toUnique "genotypeOf" "Gene"]
;;    [:from "variantIn" "Variant"]
;;    [:toUnique "variantOf" "CallSet" {:vertex.method "MUTECT"}]
;;    [:to "callSetOf" "Biosample"]
;;    [:toUnique "sampleOf" "Individual"]
;;    [:count]]))


;;   [[:toUnique "genotypeOf" "Gene"]
;;    [:from "variantIn" "Variant"]
;;    [:toUnique "variantOf" "CallSet" {:vertex.method "MUTECT"}]
;;    [:to "callSetOf" "Biosample"]
;;    [:to "sampleOf" "Individual"]
;;    [:toUnique "drugTherapyFrom" "Compound"]
;;    [:count]]


;; [{:$lookup {:from "genotypeOf", :localField "gid", :foreignField "from", :as "edge"}}
;;  {:$unwind "$edge"}
;;  {:$match {}}
;;  {:$addFields {"edge._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$edge"}}
;;  {:$group {:_id "$to", :dedup {:$first "$$ROOT"}}}
;;  {:$replaceRoot {:newRoot "$dedup"}}
;;  {:$lookup {:from "Gene", :localField "to", :foreignField "gid", :as "vertex"}}
;;  {:$unwind "$vertex"}
;;  {:$match {}}
;;  {:$addFields {"vertex._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$vertex"}}
;;  {:$lookup {:from "variantIn", :localField "gid", :foreignField "to", :as "edge"}}
;;  {:$unwind "$edge"}
;;  {:$match {}}
;;  {:$addFields {"edge._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$edge"}}
;;  {:$lookup {:from "Variant", :localField "from", :foreignField "gid", :as "vertex"}}
;;  {:$unwind "$vertex"}
;;  {:$match {}}
;;  {:$addFields {"vertex._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$vertex"}}
;;  {:$count "count"}
;;  {:$project {:_id false}}]
