{:graph  {:database :janus
          :host "localhost"
          :keyspace :pentomino}
 :search {:host "localhost"
          :port 9200
          :index :pentomino
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
         :database "pentomino"}
 :server {:port 4443}
 :protograph {:path "resources/config/protograph.yml"
              :prefix "protograph"}}
