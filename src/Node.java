import java.util.*;
import java.io.*;
import org.apache.hadoop.io.Writable;

public class Node implements Iterable<Integer>, Writable{
    int nodeid;
    double pageRank;
    int[] outgoing;

    //Here for internal Hadoop purposes only. Don't use this constructor!
    public Node() {
    	nodeid = -1;
    	outgoing = new int[0];
    }

    //Construct a node with no outgoing links.
    public Node(int nid) {
    	nodeid = nid;
    	outgoing = new int[0];
    }

    //Construct a node where the outgoing links are have nodeids in outs.
    public Node(int nid, int[] outs) {
    	nodeid = nid;
    	outgoing = outs;
    }

    //Allow iteration through the outgoing edges.
    //Used for for-each loops
    public Iterator<Integer> iterator() {
		ArrayList<Integer> al = new ArrayList<Integer>();
		for(int i : outgoing) {
		    al.add(i);
		}
		return al.iterator();
    }
    
    //Get the number of outgoing edges
    public int outgoingSize() {
    	return outgoing.length;
    }
    
    //Set the outgoing edges to be a new array
    public void setOutgoing(int[] outs) {
    	outgoing = outs;
    }
    
    //Get the PageRank of this node.
    public double getPageRank() {
    	return pageRank;
    }
    
    //Set the PageRank of this node
    public void setPageRank(double pr) {
    	pageRank = pr;
    }

    //Used for internal Hadoop purposes.
    //Describes how to write this node across a network
    public void write(DataOutput out) throws IOException {
		out.writeInt(nodeid);
		out.writeDouble(pageRank);
		for(int n : outgoing) {
		    out.writeInt(n);
		}
		out.writeInt(-1);
    }

    //Used for internal Hadoop purposes
    //Describes how to read this node from across a network
    public void readFields(DataInput in) throws IOException {
		nodeid = in.readInt();
		pageRank = in.readDouble();
		int next = in.readInt();
		ArrayList<Integer> ins = new ArrayList<Integer>();
		while (next != -1) {
		    ins.add(next);
		    next = in.readInt();
		}
		outgoing = new int[ins.size()];
		for(int i = 0; i < ins.size(); i++) {
		    outgoing[i] = ins.get(i);
		}
    }
    
    //Gives a human-readable representaton of the node.
    public String toString() {
		String retv = "Node {\n";
		retv += "\tnodeid: " + nodeid + "\n";
		retv += "\tpageRank: " + pageRank + "\n";
		retv += "\toutgoing: ";
		String out = "";
		for(int n : outgoing)
			out += "" + n + ",";
		if(!out.equals(""))
			out = out.substring(0, out.length() - 1);
		retv += out + "\n";
		retv +="}";
		return retv;
    }
}
