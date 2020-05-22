start=`date +%s`
.//home/giacomo/spark-3.0.0-preview2-bin-hadoop3.2/bin/spark-submit --master local[*] /home/giacomo/PycharmProjects/corno_grande_progetto1/job2/spark/job2_spark.py
end=`date +%s`
runtime=$((end-start))
echo $runtime