(ns build
  (:require [badigeon.javac :as j]))

(defn javac []
  (println "Compiling Java")
  (j/javac "src-java" {:compile-path "classes"
                  ;; Additional options used by the javac command
                  :compiler-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]})
  (println "Compilation Completed"))

(defn -main []
  (javac))
