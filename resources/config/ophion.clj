{:graph  {:database :janus
          :host "localhost"
          :keyspace :pentomino}
 :server {:port 4443}
 :search {:index :pentomino
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
 :protograph {:path "resources/config/protograph.yml"
              :prefix "protograph"}
 :kafka {:host "localhost"
         :port 9092}}
