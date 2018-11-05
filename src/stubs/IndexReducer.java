package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {
	  //it`s appending the values of different pllaces where the word appears
	  StringBuffer buf = new StringBuffer();
	  for (Text value : values)
	  {
		  buf.append(value); //this is where that happens
		  buf.append(';');
	  }
	  context.write(key, new Text(buf.toString()));
  }
}