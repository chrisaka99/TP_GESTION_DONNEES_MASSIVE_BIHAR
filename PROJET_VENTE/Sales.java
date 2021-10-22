/*
	M2 BIHAR - BIG DATA - HADOOP
	Année 2021/2022
  --
  TP: PROGRAMME D ANALYSE DES VENTES
  --
  Sales.java: classe driver (contient le main du programme).
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;


// la classe driver du programme
public class Sales
{
	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Créé un object de configuration Hadoop.
		Configuration conf=new Configuration();

		// Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
		String[] ourArgs=new GenericOptionsParser(conf, args).getRemainingArgs();

		//on transfere le type d analyse dans la variable de configuration d hadoop
		//il s agit du troisieme argument de la ligne de commande
		//il doit s agit d un nombre entre 0 et 3
		String analyse_type=ourArgs.length>2 ? ourArgs[2] : "0";
		int type=0;
		try{
			type=Integer.parseInt(analyse_type);
			if(type<0 && type>3)type=0;
		}
		catch(Exception e){
			type=0;
		}
		analyse_type=String.valueOf(type);
		conf.set("ANALYSE_TYPE",analyse_type);

		if(ourArgs.length>2)
			System.out.println("ANALYSE_TYPE : " +analyse_type);
		else
			System.out.println("ANALYSE_TYPE_DEFAULT : " +analyse_type);

		// Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la tâche.
		Job job=Job.getInstance(conf, "ANALYSE DE VENTE V1.0");

		// Défini les classes driver, map et reduce.
		job.setJarByClass(Sales.class);
		job.setMapperClass(SalesMap.class);
		job.setReducerClass(SalesReduce.class);

		// Défini types clefs/valeurs de notre programme Hadoop.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Défini les fichiers d'entrée du programme et le répertoire des résultats.
		// On se sert du premier et du deuxième argument restants pour permettre à l'utilisateur de les spécifier
		// lors de l'exécution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

		// On lance la tâche Hadoop. Si elle s'est effectuée correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}
