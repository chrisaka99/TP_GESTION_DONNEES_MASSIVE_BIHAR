/*
	M2 BIHAR - BIG DATA - HADOOP
	Année 2021/2022
  --
  TP: PROGRAMME D ANALYSE DES VENTES
  --
  SalesMap.java: classe Map (contient la classe Mapper du programme).
*/

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.StringTokenizer;


// Notre classe MAP.
public class SalesMap extends Mapper<Object, Text, Text, Text>
{
	// La fonction MAP elle-même.
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] list_valeur=value.toString().split(",");

		//on recupere le type d analyse a effectue
		int ANALYSE_TYPE;
		try {
			ANALYSE_TYPE=Integer.parseInt(context.getConfiguration().get("ANALYSE_TYPE"));
		}
		catch(Exception e){
			ANALYSE_TYPE=0;
		}

		String name_;//la valeur que prend la clef
		String profit=list_valeur[13];//le profit total
		if(ANALYSE_TYPE==3){
			//dans ce cas la cle est une concatenation du type de produit et de la chaine de vente
			//exemple : Beverages-Online
			name_=list_valeur[2]+'-'+list_valeur[ANALYSE_TYPE];
			//la valeur de la cle est une concatenation du produit et de la quantite vendu
			profit=profit+";"+list_valeur[8];//concatenation de la quantite et du profit
		}
		else{
			name_=list_valeur[ANALYSE_TYPE];
		}

		Text key_=new Text(name_);
		Text key_Value=new Text(profit);
		//FloatWritable float_profit=new FloatWritable(Float.parseFloat(profit));
		context.write(key_, key_Value);
	}
}
