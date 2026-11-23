package hits;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HitsDriver {

    // --- MAPPER ---
    // Objectif : pour chaque ligne de log, émettre (tranche de 15 min, 1)
    public static class HitsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable UN = new IntWritable(1);
        private static final int DEBUT = 10 * 60 + 30;   // 10h30 en minutes depuis minuit
        private static final int FIN   = 23 * 60 + 15;   // 23h15 en minutes depuis minuit

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // ligne : 79.133.215.123 - - [14/Jun/2014:10:30:13 -0400] "GET /home ..."
            String[] champs = value.toString().split(" ");
            if (champs.length < 4) return;                 // ligne bizarre : on l'ignore

            // champs[3] = "[14/Jun/2014:10:30:13" ; coupé sur ":" -> ["[14/Jun/2014","10","30","13"]
            String[] t = champs[3].split(":");
            int h = Integer.parseInt(t[1]);                // l'heure
            int min = Integer.parseInt(t[2]);              // les minutes

            int quart = min / 15 * 15;                     // 44 -> 2 -> 30 ; 45 -> 3 -> 45
            int total = h * 60 + quart;                    // début de la tranche, en minutes

            if (total < DEBUT || total > FIN) return;      // hors de la plage demandée

            context.write(new Text(String.format("%02d:%02d", h, quart)), UN);
        }
    }

    // --- REDUCER ---
    // Objectif : additionner les 1 d'une même tranche
    public static class HitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int somme = 0;
            for (IntWritable v : values) {
                somme += v.get();                          // get() : IntWritable -> int
            }
            context.write(key, new IntWritable(somme));
        }
    }

    // --- DRIVER ---
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: HitsDriver <rep source> <rep cible>\n");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(HitsDriver.class);
        job.setJobName("Hits par quart d'heure");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(HitsMapper.class);
        job.setReducerClass(HitsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}