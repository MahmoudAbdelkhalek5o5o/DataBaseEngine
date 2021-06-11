import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Hashtable;

public class GridIndex implements Serializable {
	String tableName;
	String[] order;
	String[] dataType;
	Hashtable<String, String[]> ranges;
	Vector<Object> cells;
	int[] columnsOrder;

	int maxKeysInIndexBucket;
	// Table table;

	public GridIndex(String[] columnNames, ArrayList<String[]> columnsInfo, String tablename, int[] columnsOrder,
			String[] dataType, int maxKeysInIndexBucket) {
		this.columnsOrder = columnsOrder;
		order = columnNames;
		this.tableName = tablename;
		this.dataType = dataType;
		this.maxKeysInIndexBucket = maxKeysInIndexBucket;
		ranges = new Hashtable<String, String[]>();
		for (int i = 0; i < columnNames.length; i++) {
			for (int j = 0; j < columnsInfo.size(); j++) {
				if (columnNames[i].equals(columnsInfo.get(j)[1])) {
					ranges.put(columnNames[i], divide(columnNames[i], columnsInfo.get(j)[5], columnsInfo.get(j)[6],
							columnsInfo.get(j)[2]));
					break;
				}
			}

		}
		cells = new Vector<Object>();
		createGridDynamically(cells, columnNames.length, 0);
		// insert data into the grid
	}

	public void insertRecord(ArrayList<String> record, String pageNum, int primaryKeyIndex) {
		int[] position = getPosition(record);
		String primaryKey = record.get(primaryKeyIndex);
		Bucket B = getAddToBucket(cells, position, 0);
		if (B == null) {
			B = new Bucket(primaryKey, position, maxKeysInIndexBucket, false, 0);
		}
		B.insertIntoBucket(primaryKey, tableName + pageNum);
		System.out.println(B);
		serializeBucket(B);
	}

	public void updateRecord(String primaryKey, ArrayList<String> record, ArrayList<String> oldRecord,
			int primaryKeyIndex, String pageNum) {
		int[] position = getPosition(oldRecord);
		String bucketName = tableName + displayArray(order) + displayArray(position);
		Bucket B = decerializeBucket(bucketName);
		// String pageNum = B.content.get(primaryKey);
		B.deleteFromBucket(primaryKey);
		serializeBucket(B);
		insertRecord(record, pageNum, primaryKeyIndex);
	}

	public void updatePageNumber(String primaryKey, ArrayList<String> record, String pageNum) {
		int[] position = getPosition(record);
		String bucketName = tableName + displayArray(order) + displayArray(position);
		Bucket B = decerializeBucket(bucketName);
		B.deleteFromBucket(primaryKey);
		B.insertIntoBucket(primaryKey, tableName + pageNum);
	}

	public void deleteRecord(String primaryKey, ArrayList<String> deletedRecord) {
		int[] position = getPosition(deletedRecord);
		String bucketName = tableName + displayArray(order) + displayArray(position);
		Bucket B = decerializeBucket(bucketName);
		B.deleteFromBucket(primaryKey);
		System.out.println(B + "fadFJWKMAU");
		serializeBucket(B);
	}

	public void addData(Vector<ArrayList<String>> page, String pageNum, int primaryKeyIndex) {
		for (int j = 0; j < page.size(); j++) {
			int[] position = getPosition(page.get(j));
			String primaryKey = page.get(j).get(primaryKeyIndex);
			Bucket B = getAddToBucket(cells, position, 0);
			B.insertIntoBucket(primaryKey, tableName + pageNum);
			serializeBucket(B);
			// serialize bucket

		}
//		System.out.println(this);
	}

	public Bucket getAddToBucket(Vector<Object> cells, int[] position, int index) {
		if (index == position.length - 1) {
			if (cells.get(position[index]) == null) {
				Bucket B = new Bucket(tableName, position, maxKeysInIndexBucket, false, 0);
				String bucketName = B.tableName + displayArray(order) + displayArray(B.position);
				cells.set(position[index], bucketName);
				// serializeBucket(B);

				return B;
			} else {
				// decerialize and return
				String bucketName = (String) cells.get(position[index]);
//				 B.tableName
//						+ displayArray(order) + displayArray(B.position) + "_O" + B.order;
				return decerializeBucket(bucketName);

			}

		}
		return getAddToBucket((Vector<Object>) cells.get(position[index]), position, index + 1);
	}

	public int[] getPosition(ArrayList<String> record) {
		int[] position = new int[order.length];
		for (int i = 0; i < order.length; i++) {
			String[] range = ranges.get(order[i]);
			String value = record.get(columnsOrder[i]);
			for (int j = 1; j < range.length; j++) {

				if (Table.compare(value.toLowerCase(), range[j].toLowerCase(), dataType[i]) < 0) {
					position[i] = j - 1;
					break;
				}
			}
		}
		return position;
	}

////
	protected void serialize(Vector<Vector<String>> page, int order) {
		try {

			FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + tableName + "." + order);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(page);

			out.close();

			fileOut.close();

//			if (!table.pageAddress.contains(order)) {
//				table.pageAddress.add(order);
//			}
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	protected Vector<Vector<String>> decerialize(int order) {
		Vector<Vector<String>> page = null;
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "." + order);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Vector<Vector<String>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}
		return page;
	}

	public static void createGridDynamically(Vector<Object> grid, int dimensions, int index) {

		if (index < dimensions - 1) {
			for (int i = 0; i < 10; i++) {
				Vector<Object> t = new Vector<Object>();
				createGridDynamically(t, dimensions, index + 1);
				grid.add(t);
			}
//			Vector<Object> nulls = new Vector<Object>();
//			createGridDynamically(nulls, dimensions, index + 1);
//			grid.add(nulls);
		} else {
			for (int i = 0; i < 10; i++) {
				grid.add(null);
			}
			// grid.add(null);
		}
	}

	public static String[] divide(String columnName, String min, String max, String dataType) {
		String[] currentRange = new String[11];
		for (int i = 0; i < currentRange.length; i++) {
			currentRange[i] = "";
		}
		boolean flaggy = false;
		switch (dataType) {
		case "java.lang.Integer":
			int diffInt = Integer.parseInt(max) - Integer.parseInt(min);
			if (diffInt < 10) {
				currentRange = new String[diffInt + 1];
				for (int i = 0; i < currentRange.length; i++) {
					currentRange[i] = Integer.parseInt(min) + i + "";
				}
				for (int i = 0; i < currentRange.length; i++) {
					System.out.print(currentRange[i] + ",");
				}
				return currentRange;
			}
			int roundDown = (diffInt) / 10;
			int roundup = (int) Math.ceil((diffInt) / 10.0);
			if (diffInt % 10 == 0) {
				for (int i = 0; i < currentRange.length; i++) {
					currentRange[i] = (Integer.parseInt(min) + i * roundDown) + "";
				}
			} else {
				int remainder1 = (Integer.parseInt(max) - Integer.parseInt(min)) % 10;
				for (int i = 0; i < currentRange.length; i++) {
					if (i < remainder1) {
						currentRange[i] = (Integer.parseInt(min) + i * roundup) + "";
					} else {
						currentRange[i] = (Integer.parseInt(min) + (remainder1 * roundup)
								+ (i - remainder1) * roundDown) + "";
					}
				}
			}
			break;
		case "java.lang.Double":
			Double cellRange = (Double.parseDouble(max) - Double.parseDouble(min)) / 10;
			for (int i = 0; i < currentRange.length; i++) {
				currentRange[i] = (Double.parseDouble(min) + cellRange * i) + "";
			}
			break;
		case "java.util.Date":
			long difference = ChronoUnit.DAYS.between(LocalDate.parse(min), LocalDate.parse(max));
			int diff = (int) difference;
			System.out.println(diff);
			if (diff < 10) {
				currentRange = new String[diff + 1];
				for (int i = 0; i < currentRange.length; i++) {
					currentRange[i] = LocalDate.parse(min).plusDays(i).toString();
				}
				for (int i = 0; i < currentRange.length; i++) {
					System.out.print(currentRange[i] + ",");
				}
				return currentRange;
			}
			int diff1 = diff / 10;
			int diff2 = (int) Math.ceil(diff / 10.0);
			int remainder = diff % 10;
//			System.out.println(diff);
			for (int i = 0; i < currentRange.length; i++) {
				if (i < remainder) {
					currentRange[i] = LocalDate.parse(min).plusDays(i * (int) diff2).toString();
				} else {
					currentRange[i] = LocalDate.parse(min).plusDays(remainder * diff2 + ((i - remainder) * diff1))
							.toString();
				}
				// Date after adding the days to the given date
			}
			break;
		default:

			for (int j = 0; j < min.length() && j < max.length(); j++) {
				char firstmin = (min.toLowerCase()).charAt(j);
				char firstmax = (max.toLowerCase()).charAt(j);
				int range = firstmax - firstmin;
				int lower = range / 10;
				int higher = (int) Math.ceil((range) / 10.0);
				int remain = range % 10;
				if (lower >= 1) {
					for (int i = 0; i < currentRange.length; i++) {
						if (i < remain) {
							currentRange[i] = currentRange[i].concat((char) (firstmin + i * higher) + "");
							// System.out.println(firstmin + i * higher);
						} else {
							currentRange[i] = currentRange[i]
									.concat((char) (firstmin + higher * remain + (i - remain) * lower) + "");
							// System.out.println(firstmin + higher * remain + (i - remain) * lower);
						}
					}
				} else {
					currentRange = new String[range + 1];

					for (int i = 0; i < currentRange.length; i++) {
						currentRange[i] = ((char) (firstmin + i)) + "";
					}
				}
			}
		}
		if (!flaggy) {
			for (int i = 0; i < currentRange.length; i++) {
				System.out.print(currentRange[i] + ",");
			}
		}
		System.out.println("");
		return currentRange;
	}

	public String toString() {

		String s = "";
		for (int i = 0; i < cells.size(); i++)
//		s+= getAddToBucket(cells,new int[] {i} , 0) +",";
			s += cells.get(i) + "\n";
		return s;

	}

	protected void serializeBucket(Bucket bucket) {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + bucket.tableName
					+ displayArray(order) + displayArray(bucket.position));

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(bucket);

			out.close();

			fileOut.close();
			if (bucket.isOverflow == true)
				serializeBucket(bucket.overflow);

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	protected Bucket decerializeBucket(String bucketName) {
		Bucket bucket = null;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\" + bucketName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			bucket = (Bucket) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return bucket;
	}

	private String displayArray(Object[] x) {
		String s = "";
		for (Object i : x) {
			s += "_" + i;
		}
		return s;
	}

	private String displayArray(int[] position) {

		String s = "";
		for (int i : position) {
			s += "_" + i;
		}
		return s;
	}

	public static boolean integerArraysEquals(int[] a1, int[] a2) {
		if (a1.length != a2.length)
			return false;
		for (int i = 0; i < a2.length; i++) {
			if (a1[i] != a2[i])
				return false;
		}
		return true;
	}

	public static Vector<Bucket> logicalAndBuckets(Vector<Bucket> v1, Vector<Bucket> v2) {

		Vector<Bucket> res = new Vector<Bucket>();
		if (v1 == null || v2 == null)
			return res;
		for (int i = 0; i < v1.size(); i++) {
			for (int j = 0; j < v2.size(); j++) {
				// if both buckets have the same position -> add one to the result
				if (integerArraysEquals(v1.get(i).position, v2.get(j).position))
					res.add(v1.get(i));
			}
		}
		return res;
	}

	public static Vector<Bucket> logicalOrBuckets(Vector<Bucket> v1, Vector<Bucket> v2) {
		if (v1 == null && v2 == null)
			return new Vector<Bucket>();
		else if (v1 == null)
			return v2;
		else if (v2 == null)
			return v1;

		v1.addAll(v2);
		for (int i = 0; i < v1.size() - 1; i++) {
			for (int j = i + 1; j < v1.size(); j++) {
				// if both buckets have the same position, i.e the same -> remove only one
				if (integerArraysEquals(v1.get(i).position, v1.get(j).position))
					v1.remove(i);
			}
		}
		return v1;
	}

	public static Vector<Bucket> logicalXorBuckets(Vector<Bucket> v1, Vector<Bucket> v2) {
		if (v1 == null && v2 == null)
			return new Vector<Bucket>();
		else if (v1 == null)
			return v2;
		else if (v2 == null)
			return v1;
		v1.addAll(v2);
		for (int i = 0; i < v1.size() - 1; i++) {
			for (int j = i + 1; j < v1.size(); j++) {
				// if both buckets have the same position, i.e the same -> remove both
				if (integerArraysEquals(v1.get(i).position, v1.get(j).position)) {
					v1.remove(j);
					v1.remove(i);
				}
			}
		}
		return v1;
	}

	public Vector<Bucket> doLogicalOperation(Vector<Object> cells, Vector<String> columns, String[] dataType,
			Vector<Object> values, Vector<String> mathOperator, String logicalOp) throws DBAppException {
		// get the indecies of every column using its ranges in grid
		Vector<Integer> indeces = new Vector<Integer>();
		for (int i = 0; i < columns.size(); i++) {
			String[] columnRanges = ranges.get(columns.get(i));
			indeces.add(getOnePosition(columnRanges, values.get(i) + "", dataType[i], mathOperator.get(i)));
		}

		if (logicalOp.equalsIgnoreCase("and")) {
			// if any index is -1 return empty no rows

			for (int i = 0; i < indeces.size(); i++) {
				if (indeces.get(i) == -1)
					return null;
			}
		}
		Vector<Vector<Bucket>> bucketFromEveryColumn = new Vector<Vector<Bucket>>();

		Vector<String> wrapperVectorColumnsForNewEq = new Vector<String>();
		for (int i = 0; i < order.length; i++) {

			wrapperVectorColumnsForNewEq.add(order[i]);
		}
		// we do the more/less/equal operation for each column separately then we
		// compare the buckets according to the logical op
		for (int i = 0; i < columns.size(); i++) {
			if (mathOperator.get(i).equals(">") || mathOperator.get(i).equals(">=")) {
				Vector<Bucket> tmpVector;
				Vector<String> wrapperVectorColumns = new Vector<String>();
				wrapperVectorColumns.add(columns.get(i));
				Vector<Integer> wrapperVectorIndeces = new Vector<Integer>();
				wrapperVectorIndeces.add(indeces.get(i));
				tmpVector = moreThanOperation(cells, wrapperVectorColumns, wrapperVectorIndeces);
				bucketFromEveryColumn.add(tmpVector);
			} else if (mathOperator.get(i).equals("<") || mathOperator.get(i).equals("<=")) {
				Vector<Bucket> tmpVector;
				Vector<String> wrapperVectorColumns = new Vector<String>();
				wrapperVectorColumns.add(columns.get(i));
				Vector<Integer> wrapperVectorIndeces = new Vector<Integer>();
				wrapperVectorIndeces.add(indeces.get(i));
				tmpVector = lessThanOperation(cells, wrapperVectorColumns, wrapperVectorIndeces);
				bucketFromEveryColumn.add(tmpVector);
			} else if (mathOperator.get(i).equals("!=")) {
				Vector<Bucket> tmpVector;
				Vector<String> wrapperVectorColumns = new Vector<String>();
				wrapperVectorColumns.add(columns.get(i));
				Vector<Integer> wrapperVectorIndeces = new Vector<Integer>();
				wrapperVectorIndeces.add(indeces.get(i));
				tmpVector = moreThanOperation(cells, wrapperVectorColumns, wrapperVectorIndeces);
				bucketFromEveryColumn.add(tmpVector);
				tmpVector.clear();
				wrapperVectorColumns.clear();
				wrapperVectorIndeces.clear();
				wrapperVectorColumns.add(columns.get(i));
				wrapperVectorIndeces.add(indeces.get(i));
				tmpVector = equalOperation(cells, wrapperVectorColumns, wrapperVectorIndeces);
				bucketFromEveryColumn.add(tmpVector);
			} else {
				// = case
				Vector<Bucket> tmpVector;
				Vector<String> wrapperVectorQuery = new Vector<String>();
				wrapperVectorQuery.add(columns.get(i));
				Vector<Integer> wrapperVectorIndeces = new Vector<Integer>();
				wrapperVectorIndeces.add(indeces.get(i));

				tmpVector = newEqualOp(cells, wrapperVectorColumnsForNewEq, 0, wrapperVectorQuery, wrapperVectorIndeces,
						0);
				bucketFromEveryColumn.add(tmpVector);
			}
		}

		// do the and operation on elements of bucketFromEveryColumn till there's only
		// one Vector<Bucket> left

		while (bucketFromEveryColumn.size() > 1) {
			if (logicalOp.equals("and"))
				bucketFromEveryColumn
						.add(logicalAndBuckets(bucketFromEveryColumn.get(0), bucketFromEveryColumn.get(1)));
			else if (logicalOp.equals("or"))
				bucketFromEveryColumn.add(logicalOrBuckets(bucketFromEveryColumn.get(0), bucketFromEveryColumn.get(1)));
			else
				bucketFromEveryColumn
						.add(logicalXorBuckets(bucketFromEveryColumn.get(0), bucketFromEveryColumn.get(1)));
			bucketFromEveryColumn.remove(0);
			bucketFromEveryColumn.remove(0);
		}
		// we return the buckets that satisfy the input string with its mathematical and
		// logical operators done on buckets level
		return bucketFromEveryColumn.get(0);
	}

	private int getOnePosition(String[] columnRanges, String value, String dataType, String operation) {
		int pos = -1;
		if (Table.compare(value.toLowerCase(), columnRanges[0].toLowerCase(), dataType) < 0) {
			if (operation.equals(">") || operation.equals(">=") || operation.equals("!=")) {
				// all bigger (more than 0)
				return 0;
			} else {
				return -1;
			}
		} else if (Table.compare(value.toLowerCase(), columnRanges[columnRanges.length - 1].toLowerCase(),
				dataType) > 0) {
			if (operation.equals("<") || operation.equals("<=") || operation.equals("!=")) {
				// all smaller (less than no of ranges)
				return columnRanges.length - 2;
			} else {
				return -1;
			}
		}

		for (int i = 1; i < columnRanges.length; i++) {
			if (Table.compare(value.toLowerCase(), columnRanges[i].toLowerCase(), dataType) < 0) {
				pos = i - 1;
				break;
			}
		}
		return pos;

	}

//	

	public Vector<Bucket> equalOperation(Vector<Object> grid, Vector<String> columns, Vector<Integer> index) {
		Vector<Bucket> bucket = new Vector<Bucket>();
		equalOperationHelper(grid, columns, index, 0, 0, bucket);
		return bucket;
	}

	private void equalOperationHelper(Vector<Object> grid, Vector<String> columns, Vector<Integer> index, int level,
			int currentPos, Vector<Bucket> myBucket) {
		// if at the depth and return everything
		/*
		 * if (currentPos == columns.size()) { for (Object x : grid) {
		 * myBucket.add(decerializeBucket("" + (x))); } return; }
		 */

		if ((level == order.length - 1) && !(columns.get(currentPos).equals(order[level]))) {
			for (Object x : grid) {
				Bucket b = decerializeBucket("" + (x));
				if (b != null)
					myBucket.add(b);
			}
			return;
		}
		// if the you're at the depth and want to get a specific value
		if ((level == order.length - 1) && columns.get(currentPos).equals(order[level])) {
			Bucket b = decerializeBucket("" + grid.get(index.get(currentPos)));
			if (b != null)
				myBucket.add(b);
			return;
		}
		if (!columns.get(currentPos).equals(order[level]))
			for (Object x : grid)
				equalOperationHelper((Vector<Object>) x, columns, index, level + 1, currentPos, myBucket);
		else {
			if (currentPos + 1 != columns.size())
				equalOperationHelper((Vector<Object>) grid.get(index.get(currentPos)), columns, index, level + 1,
						currentPos + 1, myBucket);
			else {
				equalOperationHelper((Vector<Object>) grid.get(index.get(currentPos)), columns, index, level + 1,
						currentPos, myBucket);
			}
		}
	}

	public Vector<Bucket> newEqualOp(Vector<Object> actualCells, Vector<String> gridColumns, int g,
			Vector<String> queryColumns, Vector<Integer> queryPos, int q) {
		// g: [x,..], q:[] - > get all the bottom (Will happen)
		if (q > queryColumns.size() - 1) {
			return toTheBottom(actualCells);
		} else if (q == queryColumns.size() - 1) {
			if (g == gridColumns.size() - 1) {
				Vector<Bucket> tmp = new Vector<Bucket>();
				Bucket b = decerializeBucket(actualCells.get(queryPos.get(q)) + "");
				if (b != null)
					tmp.add(b);
				return tmp;
			} else
				return toTheBottom((Vector<Object>) actualCells.get(queryPos.get(q)));
		} else {
			// g:[x,...], q:[x,..0] -> g:[...], q:[...]
			if (gridColumns.get(g).equals(queryColumns.get(q))) {
				return newEqualOp((Vector<Object>) actualCells.get(queryPos.get(q)), gridColumns, g + 1, queryColumns,
						queryPos, q + 1);
			} // g:[x,y,z], q:[y,..] -> loop g:[y,z], q: [y,..]
			else {
				Vector<Bucket> res = new Vector<Bucket>();
				for (int i = 0; i < actualCells.size(); i++) {
					res.addAll(newEqualOp((Vector<Object>) actualCells.get(i), gridColumns, g + 1, queryColumns,
							queryPos, q));
				}
				return res;
			}
		}
	}

	public Vector<Bucket> newMoreOp(Vector<Object> actualCells, Vector<String> gridColumns, int g,
			Vector<String> queryColumns, Vector<Integer> queryPos, int q) {
		// g: [x,..], q:[] - > get all the bottom (Will happen)
		if (q > queryColumns.size() - 1) {
			return toTheBottom(actualCells);
		}

		else if (q == queryColumns.size() - 1) {
			if (g == gridColumns.size() - 1) {
				Vector<Bucket> tmp = new Vector<Bucket>();
				for (int i = queryPos.get(q); i < 10; i++) {
					Bucket b = decerializeBucket(actualCells.get(i) + "");
					if (b != null)
						tmp.add(b);
				}
				return tmp;
			}

			else {
				if (queryColumns.get(q).equals(gridColumns.get(g))) {
					Vector<Bucket> tmp = new Vector<Bucket>();
					for (int i = queryPos.get(q); i < 10; i++) {
						tmp.addAll(newMoreOp((Vector<Object>) actualCells.get(i), gridColumns, g + 1, queryColumns,
								queryPos, q + 1));
					}
					return tmp;
				} else {
					Vector<Bucket> tmp = new Vector<Bucket>();
					for (int i = 0; i < 10; i++) {
						tmp.addAll(newMoreOp((Vector<Object>) actualCells.get(i), gridColumns, g + 1, queryColumns,
								queryPos, q));
					}
					return tmp;
				}
			}
		}
		// shouldn't happen ever because everytime we pass a single element to
		// queryColumns
		else {
//			// g:[x,...], q:[x,..0] -> g:[...], q:[...]
//			if (gridColumns.get(g).equals(queryColumns.get(q))) {
//
//				Vector<Bucket> tmp = new Vector<Bucket>();
//				for (int i = queryPos.get(q); i < 10; i++) {
//
//					tmp.addAll(newMoreOp((Vector<Object>) actualCells.get(i), gridColumns, g + 1, queryColumns,
//							queryPos, q + 1));
//				}
//
//				return tmp;
//			}
//			// g:[x,y,z], q:[y,..] -> loop g:[y,z], q: [y,..]
//			else {
//				Vector<Bucket> res = new Vector<Bucket>();
//				for (int i = 0; i < actualCells.size(); i++) {
//					res.addAll(newMoreOp((Vector<Object>) actualCells.get(i), gridColumns, g + 1, queryColumns,
//							queryPos, q));
//				}
//				return res;
//			}
			return null;
		}
	}

	private Vector<Bucket> toTheBottom(Vector<Object> actualCells) {
		Vector<Bucket> res = new Vector<Bucket>();
		try {
			for (int i = 0; i < actualCells.size(); i++) {
				Vector<Object> v = (Vector<Object>) actualCells.get(i);
				res.addAll(toTheBottom(v));
			}

		} catch (Exception e) {
			try {
				for (int j = 0; j < actualCells.size(); j++) {
					Bucket b = decerializeBucket(actualCells.get(j) + "");
					if (b != null)
						res.add(b);
				}
			} catch (Exception e2) {

			}
		}

		return res;
	}

	public Vector<Bucket> moreThanOperation(Vector<Object> grid, Vector<String> columns, Vector<Integer> index) {
		Vector<Bucket> bucket = new Vector<Bucket>();
		moreThanOperationHelper(grid, columns, index, 0, 0, bucket);
		return bucket;
	}

	private void moreThanOperationHelper(Vector<Object> grid, Vector<String> columns, Vector<Integer> index, int level,
			int currentPos, Vector<Bucket> myBucket) {

		if ((level == order.length - 1) && !(columns.get(currentPos).equals(order[level]))) {
			for (Object x : grid) {
				Bucket b = decerializeBucket((String) x);
				if (b != null)
					myBucket.add(b);
			}
			return;
		}

		if ((level == order.length - 1) && columns.get(currentPos).equals(order[level])) {
			for (int j = index.get(currentPos); j < grid.size(); j++) {
				Bucket b = decerializeBucket((String) grid.get(j));
				if (b != null)
					myBucket.add(b);
			}
			return;
		}

		if (!columns.get(currentPos).equals(order[level]))
			for (Object x : grid)
				moreThanOperationHelper((Vector<Object>) x, columns, index, level + 1, currentPos, myBucket);
		else {
			if (currentPos + 1 != columns.size())
				for (int j = index.get(currentPos); j < grid.size(); j++)
					moreThanOperationHelper((Vector<Object>) grid.get(j), columns, index, level + 1, currentPos + 1,
							myBucket);
			else
				for (int j = index.get(currentPos); j < grid.size(); j++)
					moreThanOperationHelper((Vector<Object>) grid.get(j), columns, index, level + 1, currentPos,
							myBucket);
		}
	}

	public Vector<Bucket> lessThanOperation(Vector<Object> grid, Vector<String> columns, Vector<Integer> index) {
		Vector<Bucket> bucket = new Vector<Bucket>();
		lessThanOperationHelper(grid, columns, index, 0, 0, bucket);
		return bucket;
	}

	private void lessThanOperationHelper(Vector<Object> grid, Vector<String> columns, Vector<Integer> index, int level,
			int currentPos, Vector<Bucket> myBucket) {

		if ((level == order.length - 1) && !(columns.get(currentPos).equals(order[level]))) {
			for (Object x : grid) {
				Bucket b = decerializeBucket((String) x);
				if (b != null)
					myBucket.add(b);
			}
			return;
		}

		if ((level == order.length - 1) && columns.get(currentPos).equals(order[level])) {
			for (int j = index.get(currentPos); j >= 0; j--) {
				Bucket b = decerializeBucket((String) grid.get(j));
				if (b != null)
					myBucket.add(b);
			}
			return;
		}

		if (!columns.get(currentPos).equals(order[level]))
			for (Object x : grid)
				lessThanOperationHelper((Vector<Object>) x, columns, index, level + 1, currentPos, myBucket);
		else {
			if (currentPos + 1 != columns.size())
				for (int j = index.get(currentPos); j >= 0; j--)
					lessThanOperationHelper((Vector<Object>) grid.get(j), columns, index, level + 1, currentPos + 1,
							myBucket);
			else
				for (int j = index.get(currentPos); j >= 0; j--)
					lessThanOperationHelper((Vector<Object>) grid.get(j), columns, index, level + 1, currentPos,
							myBucket);
		}
	}

	public static void main(String[] args) throws ParseException, IOException {
//		divide("kjfas", "a", "c", "java.util.Datase");// ay = a = 26^1*1 + 26^0 *25 26^1*2 + 2
		int[] a = { 1, 2, 3, 4, 5, 6 };
		int[] b = { 1, 2, 3, 4, 5, 6 };
		int[] c = a;
//		System.out.println(a.equals(b));
//		System.out.println(a.equals(c));
		System.out.println(Integer.parseInt("abcd0".substring(4)));
//		System.out.println(getLongFromString("43"));
//		System.out.println((char) (159 / 26 + 96));
//		System.out.println((char) (159 % 26 + 96));
		// divide("kjfas","15","77","java.lang.Integer");
//		Date minDate = new SimpleDateFormat("yyyy-mm-dd").parse("2010-10-05");
//		Vector<Object> ss = new Vector<Object>();
//		Vector<String []> columnsInfo = DBApp.getColumnInfo("students");
//		String[] columnNames = { "first_name" };
//		String[] dataType = { "java.lang.String" };
//		int[] columnsOrder = { 2 };
//		GridIndex grid = new GridIndex(columnNames, columnsInfo, "students", columnsOrder, dataType);
//		System.out.println(grid);
//		DBApp.serializeTabl();
//		decerializeTabl("");
		// createGridDynamically(ss, 3, 0);
		// System.out.println(ss);
		// System.out.println(LocalDate.parse("2000-01-01").plusDays(734).toString());
//		Calendar c = Calendar.getInstance();
//    	//Setting the date to the given date
//    	c.setTime(minDate);
//    	c.add(Calendar.DAY_OF_YEAR, 30);  
		// System.out.println(new SimpleDateFormat("yyyy-mm-dd").format(c.getTime()));
//		char ch = (char)115;
//		System.out.println(ch);
//		String string ="zzzzzz";
//		char cha = string.charAt(0);
//		System.out.println(cha-ch);
//		

	}

}