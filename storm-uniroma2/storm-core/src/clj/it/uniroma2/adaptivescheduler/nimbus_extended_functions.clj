(ns it.uniroma2.adaptivescheduler.nimbus-extended-functions
  (:import [it.uniroma2.adaptivescheduler CentralizedCleaner])
  ;; (:gen-class 
  )


(defn create-centralized-cleaner [conf]
  (CentralizedCleaner. conf)
  )