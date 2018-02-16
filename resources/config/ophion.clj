{:graph  {:database :janus
          :host "localhost"
          :keyspace :color}
 :search {:host "localhost"
          :port 9200
          :index :color
          :indexed?
          #{"Individual"
            "Evidence"
            "Biosample"
            "Project"
            "Cohort"
            "Gene"
            "GeneFamily"
            "GeneDatabase"
            "OntologyTerm"
            "Compound"}}
 :kafka {:host "localhost"
         :port 9092}
 :mongo {:host "127.0.0.1"
         :port 27017
         :database "color"}
 :server {:port 4443}
 :protograph {:path "../biostream/bmeg-etl/bmeg.protograph.yaml"
              :prefix "protograph"}}
