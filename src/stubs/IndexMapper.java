package stubs;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.StringBuffer;

public class IndexMapper extends Mapper<Text, Text, Text, Text>
{

  @Override
  public void map(Text key, Text value, Context context) throws IOException, InterruptedException
  {
	  FileSplit FS = (FileSplit) context.getInputSplit();
	  Path path = FS.getPath();
	  String fileName = path.getName();
	  
	  //we are trying to establish were every word appears
	  //the word is the key
	  //and the filename is the name of the file where it appears
	  //the new value is the place wehre e ih
	  String line = value.toString();
	  StringBuffer buffer = new StringBuffer();
	  buffer.append(fileName);
	  buffer.append('@');
	  buffer.append(key);
	  Text newValue = new Text(buffer.toString());
	  for (String word : line.split("\\W+"))
	  {
	      if (word.length() > 0)
	      {
	    	  context.write(new Text(word), newValue); //newvalue = fileName+"@"+key;
	      }
	  }
  }
}