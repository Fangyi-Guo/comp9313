

/*
 * this assignment will take a .dat file with data format: user::movie::rating::timestamp
 * we need to manipulate these data with mapreduce to obtain (movie1, movie2)[(user1, rating1, rating2), (user2, rating1,rating2)]
 * use joining and chaining
 * no more than two map and two reduce methods
 * complex object used to store a list of writable
 * */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class AssigOnez5175113 {
	//User::Movie::rating::11111111
	
	//user rating1 rating2
	public static class UserRateWritable implements Writable {
		private Text User;
		private IntWritable rating1;
		private IntWritable rating2;
		
		public UserRateWritable() {
			this.User = new Text("");
			this.rating1 = new IntWritable(-1);
			this.rating2 = new IntWritable(-1);
		}
		
		public UserRateWritable(String User,IntWritable rating1, IntWritable rating2) {
			super();
			this.User = new Text(User);
			this.rating1 = rating1;
			this.rating2 = rating2;
		}

		public String getUser() {
			return this.User.toString();
		}
		
		public void setUser(String User) {
			this.User = new Text(User);
		}
		
		public void setRating1(IntWritable rating1) {
			this.rating1 = rating1;
		}

		public IntWritable getRating1() {
			return this.rating1;
		}
		
		public void setRating2(IntWritable rating2) {
			this.rating2 = rating2;
		}

		public IntWritable getRating2() {
			return this.rating2;
		}

		
		@Override
		public void readFields(DataInput data) throws IOException {
			// TODO Auto-generated method stub
			this.User.readFields(data);
			this.rating1.readFields(data);
			this.rating2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			// TODO Auto-generated method stub
			this.User.write(data);;
			this.rating1.write(data);
			this.rating2.write(data);
		}
		
		public String toString() {
			return "("+this.User.toString() + ", "+ this.rating1.toString()+", "+ this.rating2.toString()+ ")";
		}
		
		public UserRateWritable toUserRateWritable(String line) {
			UserRateWritable re = new UserRateWritable();
			String[] parts = line.split(", ");
			re.setUser(parts[0].substring(1));
			re.setRating1(new IntWritable(Integer.parseInt(parts[1])));
			re.setRating2(new IntWritable(Integer.parseInt(parts[2].substring(0, (parts[2].length())-1))));
			return re;
		}
	}
	
	//movie pair
	public static class MoviePair implements WritableComparable<MoviePair> {
		private Text movie1;
		private Text movie2;
		
		public MoviePair() {
			this.movie1 = new Text("");
			this.movie2 = new Text("");
		}
        
        public MoviePair(String first, String second)
        {
        	super();
        	this.movie1 = new Text(first);
            this.movie2 = new Text(second);
        }
        
        public void setMovie1(String movie1) {
        	this.movie1 = new Text(movie1);
        }
        
        public void setMovie2(String movie2) {
        	this.movie2 = new Text(movie2);
        }
        
        public Text getMovie1() {
        	return this.movie1;
        }
        
        public Text getMovie2() {
        	return this.movie2;
        }
        
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.movie1.readFields(in);
			this.movie2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			this.movie1.write(out);
			this.movie2.write(out);
		}

		
		public String toString() {//filter out the repeated stuff
			if(this.movie1.toString().compareTo(this.movie2.toString()) > 0) {
				return "("+this.movie1.toString()+ ", "+ this.movie2.toString()+")";
			}
			else  {
				return "("+this.movie2.toString()+ "," + this.movie1.toString() + ")";
			}
			
		}
		
		public MoviePair toMoviePair(String input) {
			String[] parts = input.split(",");
			String m1 = parts[0].substring(1);
			String m2 = parts[1].substring(0, parts[1].length()-1);
			MoviePair re = new MoviePair(m1,m2);
			return re;
		}

		@Override
		public int compareTo(MoviePair o) {
			// TODO Auto-generated method stub
			return o.toString().compareTo(this.toString());
		}
		
		
		
	}
	
	
	/*public static class MovieSets{
		List<MoviePair> lists =new ArrayList<MoviePair>();
		
		public MovieSets() {
		}
		
		public MovieSets(List<MoviePair> lists) {
			for(MoviePair l:lists) {
				this.lists.add(l);
			}
		}
		
		public void addMovie(MoviePair movie) {
			this.lists.add(movie);
		}
		
		public void removeMovie(MoviePair movie) {
			this.lists.remove(movie);
		}
		
	}*/
	
	public static class ratingSets{
		List<UserRateWritable> lists =new ArrayList<UserRateWritable>();
		
		public ratingSets() {
		}
		
		public ratingSets(List<UserRateWritable> lists) {
			for(UserRateWritable l:lists) {
				this.lists.add(l);
			}
		}
		
		public void addRate(UserRateWritable rate) {
			this.lists.add(rate);
		}
		
		public void removeRate(UserRateWritable rate) {
			this.lists.remove(rate);
		}
		
		public String toString() {
			String re = "[";
			int i = 0;
			for(UserRateWritable a:this.lists) {
				re = re +  a.toString() ;
				if(i != this.lists.size()-1) {
					re = re+ ",";
				}
				i++;
			}
			re = re + "]";
			return re;
		}
		
	}
	
	/**
	 * Map string using user as key, movie and rating as value in text
	 * @author z5175113
	 *
	 */
	public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("::");
			Text val = new Text(parts[1] + " "+ parts[2]);
			context.write(new Text(parts[0]),val);
			
		}
	}
	
	/**
	 * reduce to two movie pair as key, and one user give them the ratings
	 * @author z5175113
	 *
	 */
	public static class MoviePairReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			//MovieSets movieSets = new MovieSets();
			
			UserRateWritable value = new UserRateWritable();
			MoviePair pair = new MoviePair();
			List<Text> newvalue = new ArrayList<Text>();
			while(values.iterator().hasNext()) {//put iterable into a list
				Text v = values.iterator().next();
				newvalue.add(new Text(v.toString()));
			}
			
			
			for(int i = 0; i < newvalue.size(); i++) {
				for(int j = i+1; j<newvalue.size(); j++) {
					//only compare with the next one
					if (newvalue.get(i).toString().compareTo(newvalue.get(j).toString())!=0) {
						//System.out.println(newvalue.get(i).toString() + "\n");
						value.setRating1(new IntWritable(Integer.parseInt(newvalue.get(i).toString().split(" ")[1])));
						value.setRating2(new IntWritable(Integer.parseInt(newvalue.get(j).toString().split(" ")[1])));
					
						value.setUser(key.toString());
						
						pair.setMovie1(newvalue.get(i).toString().split(" ")[0]);
						pair.setMovie2(newvalue.get(j).toString().split(" ")[0]);
						//movieSets.addMovie(pair);
						context.write(new Text(pair.toString()), new Text(value.toString()));
					}
				}
			}
			
		}
		
	}
	
	public static class RateListMapper extends Mapper<LongWritable, Text, Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
			
			context.write(new Text(parts[0]),new Text(parts[1]));
		}
	}
	
	/**
	 * combine user rating to users' rating list for the pair of movies
	 * @author z5175113
	 *
	 */
	public static class RateListReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			ratingSets ratesW = new ratingSets();
			for(Text u: values) {//put into Complex object
				//System.out.println(u.toString()+"\n");
				UserRateWritable user = new UserRateWritable();
				user = user.toUserRateWritable(u.toString());
				ratesW.addRate(user);
			}
			
			context.write(new Text(key.toString()), new Text(ratesW.toString()));
			
		}
		
	}
	
	
	
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		
		//System.out.println(out+" 1\n");
		Job job1 = Job.getInstance(conf, "Movie Pair");
		job1.setInputFormatClass(TextInputFormat.class);;
		job1.setJarByClass(AssigOnez5175113.class);
	    job1.setMapperClass(MovieMapper.class);
	    //job1.setCombinerClass(MoviePairReducer.class);
	    job1.setReducerClass(MoviePairReducer.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);
	    
	    
	    
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));//put to out which will be the input of the next job
	    job1.waitForCompletion(true);
	    
	    
	    Job job2 = Job.getInstance(conf, "user rating list");

	    job2.setInputFormatClass(TextInputFormat.class);
	    //System.out.println("hello3\n");
	    job2.setJarByClass(AssigOnez5175113.class);
	   
	    //job1.setCombinerClass(RateListReducer.class);
	    job2.setReducerClass(RateListReducer.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(RateListMapper.class);
		
	    FileInputFormat.addInputPath(job2, new Path(out, "out1"));
	    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
	    //System.out.println("hello4\n");
	    job2.waitForCompletion(true);
	}
	
}
