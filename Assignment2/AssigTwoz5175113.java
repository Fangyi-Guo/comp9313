import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class AssigTwoz5175113 {
	public static class EdgesandNodes{
		public static List<Edge> edges = new ArrayList<Edge>();
		public static List<Node> nodes = new ArrayList<Node>();
		public EdgesandNodes() {
		}
		
		public EdgesandNodes(List<Edge> edges, List<Node> nodes) {
			for(Edge e:edges) {
				EdgesandNodes.edges.add(e);
			}
			for(Node n:nodes) {
				EdgesandNodes.nodes.add(n);
			}
		}
		
		//get all sources of all edges
		public static List<Node> getSources(){
			List<Node> source = new ArrayList<Node>();
			for(Edge e: EdgesandNodes.edges) {
				source.add(e.getSource());
			}
			
			return source;
		}
		
		public static Edge getEdgebyNodes(Node src, Node dest) {
	    	for(Edge e: EdgesandNodes.edges) {
	    		if(e.getSource().getId() == src.getId() && e.getDestination().getId() == dest.getId()) {
	    			return e;
	    		}
	    	}
	    	return null;
	    }
		
	}
	
	public static class Node{
		private final String id;
		private boolean isVisited;
		List<Node> neighbors = new ArrayList<Node>();
		
		public Node(String id) {
			this.id = id;
			this.isVisited = false;
		}
		
		public String getId() {
			return this.id;
		}
		
		public void addNeighbor(Node node) {
			this.neighbors.add(node);
		}
		
		public List<Node> getNeighbors() {
			return this.neighbors;
		}
		
		public boolean getIsVisited() {
			return this.isVisited;
		}
		
		public void setIsVisited(boolean v) {
			this.isVisited = v;
		}
		
		public List<Path> findPaths(Path path, Node node) {
			List<Path> paths = new ArrayList<Path>();
			if(node.getNeighbors().size() == 0) {//has no neighbor
				//path end here;
				return paths;
			}
			else {
				List<Node> neighbors = node.getNeighbors();
				for(Node n: neighbors) {
					if(!n.isVisited) {
						n.setIsVisited(true);
						Path subpath = new Path(path.getNodes());
						List<Path> ps = findPaths(subpath, n);
						for(Path p:ps) {
							paths.add(p);
						}
					}
				}
			}
			return paths;
		}
		
		public String toString() {
			return this.getId();
		}
		
	}
	
	public static class Edge{
	    private final Node source;
	    private final Node destination;
	    private final int weight;
	    
		public Edge(){
			this.source = new Node("N0");
			this.destination = new Node("N0");
			this.weight = 0;
		}
		
		public Edge(Node source, Node destination, int weight) {
	        this.source = source;
	        this.destination = destination;
	        this.weight = weight;
	    }
		
	    public Node getDestination() {
	        return this.destination;
	    }

	    public Node getSource() {
	        return this.source;
	    }
	    public int getWeight() {
	        return this.weight;
	    }

	    @Override
	    public String toString() {
	        return this.source + "-" + this.destination;
	    }
		
	}
	
	public static class Path{
		List<Node> nodes = new ArrayList<Node>();
		
		public Path() {
		}
		
		public Path(List<Node> nodes) {
			for(Node n: nodes) {
				this.nodes.add(n);
			}
		}
		
		public List<Node> getNodes(){
			return this.nodes;
		}
		
		public void addNode(Node node) {
			this.nodes.add(node);
		}
		
		public Node getLastNode() {
			return this.nodes.get(this.nodes.size()-1);
		}
		
		public int getPathWeight() {
			int weight = 0;
			for(int i = 0; i < this.getNodes().size()-1;i++) {
				Edge n = EdgesandNodes.getEdgebyNodes(this.getNodes().get(i), this.getNodes().get(i+1));
				weight = weight+n.getWeight();
			}
			return weight;
		}
		
		public String toString() {
			String res = "";
			int i = 1;
			for(Node n : this.nodes) {
				res = res + n.getId();
				if(i != this.nodes.size()) {
					res = res + "-";
				}
				i++;
			}
			return res;
		}
	}
	
	//sort by path weight
	public static class WeightCompartor implements Comparator<Integer>, Serializable {

		@Override
		public int compare(Integer o1, Integer o2) {
			return o1- o2;
		}
		
	}
	
	//node weight path
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("AssigTwoz5175113");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		//split the line in to node, node edge
		JavaRDD<Edge> edges = context.textFile(args[0]).map(new Function<String, Edge>(){
			@Override
			public Edge call(String line) throws Exception {
				String [] parts = line.split(",");
				String node1 = parts[0];//first node
				String node2 = parts[1];//second node
				Integer edgeW = Integer.parseInt(parts[2]);//edge weight between two nodes
				Node src = new Node(node1);
				Node dest = new Node(node2);
				src.addNeighbor(dest);
				if(EdgesandNodes.nodes.contains(src))
				EdgesandNodes.nodes.add(src);
				EdgesandNodes.nodes.add(dest);
				Edge e = new Edge(src,dest,edgeW);
				EdgesandNodes.edges.add(e);
				return e;
			}
			
		});
		//change to node weight, path then get a list of Paths
		JavaRDD<Path> paths = edges.flatMap(new FlatMapFunction<Edge, Path>(){
			@Override
			public Iterator<Path> call(Edge e)
					throws Exception {
				//get the starting node from input
				Path path = new Path();
				List<Path> paths = new ArrayList<Path>();
				for(Node node: EdgesandNodes.nodes) {
					if(node.getId().equals(args[2])) {
						paths = node.findPaths(path, node);
						break;
					}
				}
				//get the shortest path for each end node in the paths
				
				return paths.iterator();//last node and path
			}
			
		});
		
		paths.mapToPair(new PairFunction<Path, Node, Tuple2<Integer, Path>>(){

			@Override
			public Tuple2<Node, Tuple2<Integer, Path>> call(Path arg0) throws Exception {
				Node lastNode = arg0.getLastNode();
				int weight = arg0.getPathWeight();
				Tuple2<Integer, Path> value = new Tuple2<Integer, Path>(weight, arg0);
				return new Tuple2<Node, Tuple2<Integer, Path>>(lastNode, value);
			}
		})//chose the shortest path to node path
		.reduceByKey(new Function2<Tuple2<Integer, Path>, Tuple2<Integer, Path>, Tuple2<Integer, Path>>() {
			@Override
			public Tuple2<Integer, Path> call(Tuple2<Integer, Path> a, Tuple2<Integer, Path> b) throws Exception {
				int weightA = a._2().getPathWeight();
				int weightB = b._2().getPathWeight();
				if(weightA > weightB) {
					return b;
				}
				else {
					return a;
				}
			}
		})//weight node path
		.mapToPair(new PairFunction<Tuple2<Node, Tuple2<Integer, Path>>, Integer, Tuple2<Node, Path>>() {
			@Override
			public Tuple2<Integer, Tuple2<Node, Path>> call(Tuple2<Node, Tuple2<Integer, Path>> triple) throws Exception {
				Node n = triple._1();
				int weight = triple._2()._1();
				Path path = triple._2()._2();
				Tuple2<Node, Path> t = new Tuple2<Node, Path>(n, path);
				return new Tuple2<Integer, Tuple2<Node, Path>>(weight, t);
			}
		})
		.sortByKey(new WeightCompartor())
		.mapToPair( new PairFunction<Tuple2<Integer, Tuple2<Node, Path>>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<Integer, Tuple2<Node, Path>> arg0) throws Exception {
				int weight = arg0._1();
				Node n = arg0._2()._1();
				Path p = arg0._2()._2();
				String path = p.toString();
				String node = n.toString();
				String t = String.valueOf(weight)+" "+path;
				return new Tuple2<String, String>(node, t);
			}
		})
		.saveAsTextFile(args[1]);
	}
	
	
}