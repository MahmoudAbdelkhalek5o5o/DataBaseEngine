import java.io.Serializable;
import java.util.TreeMap;

public class Bucket implements Serializable {
	String tableName;// in format tableName.range1/range2
	int[] position;
	TreeMap<String, String> content;
	int maxKeysInIndexBucket;
	Bucket overflow;
	int order;// first overflow takes position 1, second takes 2 ..etc
	boolean isOverflow = false;

	public Bucket(String tableName, int[] position, int maxKeysInIndexBucket, boolean isOverflow, int order) {
		this.tableName = tableName;
		this.position = position;
		// System.out.println(maxKeysInIndexBucket);
		this.maxKeysInIndexBucket = maxKeysInIndexBucket;
		this.isOverflow = isOverflow;
		this.order = order;
		content = new TreeMap<String, String>();
	}

//
	public int getBucketOrderForPrimaryKey(String primaryKey) {
		// returns -1 if not found

		if (content.containsKey(primaryKey))
			return order;
		else if (overflow != null)
			return overflow.getBucketOrderForPrimaryKey(primaryKey);
		else
			return -1;

	}

	public String getpageAddress(String primaryKey) {
		int searchOrder = getBucketOrderForPrimaryKey(primaryKey);
		if (searchOrder != -1) {
			if (searchOrder == order)
				return content.get(primaryKey);
			else
				return overflow.getpageAddress(primaryKey);
		}
		return null;
	}

	public void insertIntoBucket(String primaryKey, String pageAddress) {
		if (getBucketOrderForPrimaryKey(primaryKey) == -1) {
			if (content.size() >= maxKeysInIndexBucket) {
				if (overflow == null)
					overflow = new Bucket(tableName, position, maxKeysInIndexBucket, true, order + 1);
				overflow.insertIntoBucket(primaryKey, pageAddress);
			} else {
				// simply add the element at the end of the bucket unsorted
				// add elements belonging to the same page after each other ? wdy think?
				content.put(primaryKey, pageAddress);
//				System.out.println(content);
			}
		} else
			System.out.println("already exists.");
	}

//	

	public void deleteFromBucket(String primaryKey) {
		int searchOrder = getBucketOrderForPrimaryKey(primaryKey);
		if (searchOrder != -1) {
			if (searchOrder == order)
				content.remove(primaryKey);
			else {
				overflow.deleteFromBucket(primaryKey);
				// if we delete all page addresses from a bucket it will still be there
			}

		}

	}

	public void updateBucketValue(String primaryKey, String value) {
		// do we need to add tableName before the vale here as well ?
		int searchOrder = getBucketOrderForPrimaryKey(primaryKey);
		if (searchOrder != -1) {
			if (searchOrder == order)
				content.replace(primaryKey, value);
			else
				overflow.updateBucketValue(primaryKey, value);
		}
	}

	public String positionInFormat() {
		String res = "";
		for (int i = 0; i < position.length; i++) {
			res += position[i] + ".";
		}
		return res;
	}

//

	public String toString() {
		return content.toString() + ((overflow == null) ? "" : overflow);
//		return positionInFormat();
	}

	public static void main(String[] args) {
		Bucket a = new Bucket("a", new int[] { 1, 1 }, 6, false, 0);

		for (int i = 0; i < 10; i++)
			a.insertIntoBucket(('a' + i) + "", i + "");
		System.out.println(a);

	}
}
