package anagramme;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.TreeSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramDriver {

    // --- MAPPER ---
    // Objectif : pour chaque mot, émettre (lettres triées sans accent, mot avec accent)
    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String ligne = value.toString();
            for (String mot : ligne.split("\\s+")) {
                mot = mot.trim().toLowerCase().replaceAll("[^a-zàâäéèêëîïôöùûüç]", "");

                if (mot.length() > 0) {
                    // CHANGÉ : on retire les accents seulement pour construire la clé
                    String sansAccent = Normalizer.normalize(mot, Normalizer.Form.NFD)
                                                  .replaceAll("\\p{M}", "");
                    char[] lettres = sansAccent.toCharArray();
                    Arrays.sort(lettres);
                    String signature = new String(lettres);

                    // la valeur garde le mot d'origine, avec ses accents
                    context.write(new Text(signature), new Text(mot));
                }
            }
        }
    }

    // --- REDUCER ---
    // Objectif : pour chaque clé, lister les mots DISTINCTS, et ne garder que les groupes d'au moins 2 mots
    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // CHANGÉ : un TreeSet supprime les doublons et trie les mots
            TreeSet<String> mots = new TreeSet<String>();
            for (Text val : values) {
                mots.add(val.toString());   // toString() : Hadoop réutilise l'objet Text
            }

            // CHANGÉ : on teste le nombre de mots distincts, pas le nombre d'occurrences
            if (mots.size() > 1) {
                context.write(key, new Text(": " + String.join(" ", mots)));
            }
        }
    }

    // --- DRIVER (Main) ---
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: AnagramDriver <rep source> <rep cible>\n");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(AnagramDriver.class);
        job.setJobName("Anagram Finder");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }
}