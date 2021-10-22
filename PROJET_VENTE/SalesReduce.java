/*
	M2 BIHAR - BIG DATA - HADOOP
	Année 2021/2022
  --
  TP: PROGRAMME D ANALYSE DES VENTES
  --
  SalesReduce.java: classe Reduce (contient la classe Reduce du programme).
*/

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE - templatée avec un type générique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class SalesReduce extends Reducer<Text, Text, Text, Text>
{
	// La fonction REDUCE elle-même. Les arguments: la clef key (d'un type générique K), un Iterable de toutes les valeurs
	// qui sont associées à la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le résultat à Hadoop).
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {
		// Pour parcourir toutes les valeurs associées à la clef fournie.
		Iterator<Text> i=values.iterator();
		float profit_total=0f;
		int qte_total=0;
		int analyse_type=Integer.parseInt(context.getConfiguration().get("ANALYSE_TYPE"));
		while(i.hasNext())
		{
			// Pour chaque valeur...
			String[] value_=i.next().toString().split("\\;");
			if(analyse_type<3){
				profit_total+=Float.parseFloat(value_[0]);
			}
			else{
				profit_total=profit_total+Float.parseFloat(value_[0]);
				qte_total+=Integer.parseInt(value_[1]);
			}
		}
	  	if(analyse_type<3){
			context.write(key, new Text(String.format("=> Profit Total : %.2f Euro",profit_total)));
		}
		else{
			context.write(key, new Text(String.format(" => Quantite Total Vendu : %d => Profit Total : %.2f Euro",qte_total,profit_total)));
		}
  }
}