import org.apache.hadoop.io.Writable;
import java.io.*;

// Node + Integer (Either Node Integer, in Haskell)
public class NodeOrDouble implements Writable{
    private Node n;
    private Double d;
    private boolean is_node;

    //Used for internal Hadoop purposes only. 
    //Do not use this constructor!
    public NodeOrDouble() {
		is_node = false;
		d = -1.0;
    }

    //Construct a NodeOrDouble that is a node.
    public NodeOrDouble(Node n) {
		this.n = n;
		is_node = true;
    }
    
    //Construct a NodeOrDouble that is a Double
    public NodeOrDouble(Double d) {
		this.d = d;
		is_node = false;
    }

    //Find out whether this is actually a Node or not
    //If not, it's a Double
    public boolean isNode() {
    	return is_node;
    }

    //If this is a Node, return it.
    //Otherwise, return null
    public Node getNode() {
    	if(!isNode()) return null;
			return n;
    }
    
    //If this is a Double, return it.
    //Otherwise, return null
    public Double getDouble() {
		if(isNode()) return null;
		return d;
    }

    //Used for internal Hadoop purposes only
    //Describes how to write NodeOrDouble objects across a network
    public void write(DataOutput out) throws IOException {
		out.writeBoolean(is_node);
		if(is_node) {
		    n.write(out);
		}
		else {
		    out.writeDouble(d);
		}
    }

    //Used for internal Hadoop purposes only
    //Describes how to read NodeOrDouble objects from across a network
    public void readFields(DataInput in) throws IOException {
		is_node = in.readBoolean();
		if(is_node) {
		    n = new Node(-1); //just to avoid errors --- wish this was static
		    n.readFields(in);
		} else {
		    d = in.readDouble();
		}
    }
}
