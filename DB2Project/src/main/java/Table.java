import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

//
public class Table implements Serializable {

	protected String name;
	protected int clusteringKeyIndex;
	protected String clusterKeyType;
	private int maxRowsInPage;

	private int maxAddress;
	public ArrayList<Integer> pageAddress;
	protected ArrayList<String> min;
	protected ArrayList<String> max;
	protected ArrayList<Integer> count;

	protected Table(String tableName, int maxRowsInPage) {
		this.name = tableName;
		this.maxRowsInPage = maxRowsInPage;
		pageAddress = new ArrayList<Integer>();
		min = new ArrayList<String>();
		max = new ArrayList<String>();
		count = new ArrayList<Integer>();
		maxAddress = 0;

	}

	protected void serialize(Vector<ArrayList<String>> page, int order) {
		try {

			FileOutputStream fileOut = new FileOutputStream("src/main/resources/data/" + name + order);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(page);

			out.close();

			fileOut.close();

			if (!pageAddress.contains(order)) {
				pageAddress.add(order);
			}
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Vector<ArrayList<String>> test() {

		return decerializeStatic("students0");
	}

	protected static Vector<ArrayList<String>> decerializeStatic(String inp) {
		Vector<ArrayList<String>> page = null;
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + inp);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Vector<ArrayList<String>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}
		return page;
	}

	protected Vector<ArrayList<String>> decerialize(int order) {
		Vector<ArrayList<String>> page = null;
		try {
			FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + name + order);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Vector<ArrayList<String>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
		}
		return page;
	}

	protected Object[] insertInPage(int pageNum, ArrayList<String> record) throws IOException {
		Object shiftedRow = null;
		if (pageNum == -1 || pageNum > pageAddress.size() - 1) {
			// inserting in new page
			Vector<ArrayList<String>> page = new Vector<ArrayList<String>>(maxRowsInPage);
			page.add(record);
			pageAddress.add(maxAddress++);
			count.add(1);
			min.add(record.get(clusteringKeyIndex));
			max.add(record.get(clusteringKeyIndex));

			serialize(page, pageNum == -1 ? pageAddress.get(0) : pageAddress.get(pageNum));
			return pageNum == -1 ? new Object[] { 0, shiftedRow } : new Object[] { pageNum, shiftedRow };
		} else {
			// inserting in an existing page
			if (count.get(pageNum) < maxRowsInPage) {
				// if the page has space
				Vector<ArrayList<String>> page = decerialize(pageAddress.get(pageNum));

				page.add(binarySearch2(clusterKeyType, page, record.get(clusteringKeyIndex)), record);
				serialize(page, pageAddress.get(pageNum));
				count.set(pageNum, count.get(pageNum) + 1);
				if (compare(record.get(clusteringKeyIndex), min.get(pageNum), clusterKeyType) < 0)
					min.set(pageNum, record.get(clusteringKeyIndex));
				if (compare(record.get(clusteringKeyIndex), max.get(pageNum), clusterKeyType) > 0)
					max.set(pageNum, record.get(clusteringKeyIndex));
				return new Object[] { pageNum, shiftedRow };
			} else {
				// if the current page has no space
				// insert in the current page if it's less than maximum or else insert into next
				// page
				if (compare(record.get(clusteringKeyIndex), (max.get(pageNum)), clusterKeyType) < 0) {
					Vector<ArrayList<String>> page = decerialize(pageAddress.get(pageNum));
					ArrayList<String> recordTemp = page.remove(page.size() - 1);

					page.add(binarySearch2(clusterKeyType, page, record.get(clusteringKeyIndex)), record);
					serialize(page, pageAddress.get(pageNum));
					min.set(pageNum, page.get(0).get(clusteringKeyIndex));
					max.set(pageNum, page.get(page.size() - 1).get(clusteringKeyIndex));
					// if there's a page next to this
					if (pageNum < count.size() - 1) {

						// if next page has space
						if (count.get(pageNum + 1) < maxRowsInPage) {
							insertInPage(pageNum + 1, recordTemp);
							shiftedRow = recordTemp;
							// TODO
						} else {
							// next page has no space -> create overflow
							Vector<ArrayList<String>> page2 = new Vector<ArrayList<String>>();
							page2.add(recordTemp);

							count.add(pageNum + 1, 1);
							min.add(pageNum + 1, recordTemp.get(clusteringKeyIndex));
							max.add(pageNum + 1, recordTemp.get(clusteringKeyIndex));
							pageAddress.add(pageNum + 1, maxAddress++);

							serialize(page2, pageAddress.get(pageNum + 1));
							// TODO the shifted record goes here
							shiftedRow = recordTemp;
						}
					} else {
						// no page next to this -> create a new page
						Vector<ArrayList<String>> page2 = new Vector<ArrayList<String>>();
						page2.add(recordTemp);

						count.add(1);
						min.add(recordTemp.get(clusteringKeyIndex));
						max.add(recordTemp.get(clusteringKeyIndex));
						pageAddress.add(maxAddress++);

						serialize(page2, pageAddress.get(pageAddress.size() - 1));
						// TODO the shifted record goes here
						shiftedRow = recordTemp;
					}
					return new Object[] { pageNum, shiftedRow };

				} else
					insertInPage(pageNum + 1, record);
			}
		}
		return new Object[] { -1, shiftedRow };
	}

	protected static int compare(String s, String ss, String dataType) {
		switch (dataType) {
		case "java.lang.Double":
			double sDouble = Double.parseDouble(s);
			double ssDouble = Double.parseDouble(ss);
			if (sDouble > ssDouble)
				return 1;
			else if (sDouble < ssDouble)
				return -1;
			else
				return 0;
		case "java.util.Date":
			return s.compareTo(ss);

		case "java.lang.Integer":
			int sInt = Integer.parseInt(s);
			int ssInt = Integer.parseInt(ss);
			if (sInt > ssInt)
				return 1;
			else if (sInt < ssInt)
				return -1;
			else
				return 0;
		default:
			while (s.length() > 1) {
				if (s.charAt(0) == '0')
					s = s.substring(1);
				else
					break;
			}
			while (ss.length() > 1) {
				if (ss.charAt(0) == '0')
					ss = ss.substring(1);
				else
					break;
			}
			if (s.length() == ss.length())
				return s.compareTo(ss);
			return (s.length() > ss.length()) ? 1 : -1;
		}
	}

	protected int binarySearch2(String dataType, Vector<ArrayList<String>> page, String val) {
		int l = 0, r = page.size() - 1;
		int m;

		// page has many array lists each of which is a row in the table

		while (l <= r) {

			m = l + (r - l) / 2;

			ArrayList<String> a = (ArrayList<String>) page.get(m);
			// Check if x is present at mid
			if (a.get(clusteringKeyIndex).equals(val))
				return m;

			// If x greater, ignore left half
			if (compare(a.get(clusteringKeyIndex), (val), dataType) < 0) {
				l = m + 1;
				if (l < page.size()
						&& compare((((ArrayList<String>) page.get(l)).get(clusteringKeyIndex)), (val), dataType) > 0) {
					return l;
				}

			}
			// If x is smaller, ignore right half
			else {
				r = m - 1;
				if (r > -1
						&& compare((((ArrayList<String>) page.get(r)).get(clusteringKeyIndex)), (val), dataType) < 0) {
					return m;
				}
			}
		}

		if (compare(val, (((ArrayList<String>) page.get(page.size() - 1)).get(clusteringKeyIndex)), dataType) > 0)
			return page.size();

		else if (compare(val, (((ArrayList<String>) page.get(0)).get(clusteringKeyIndex)), dataType) < 0) {
			return 0;
		}

		return -1;
	}

	protected int[] binarySearch(Vector<ArrayList<String>> page, String val, String dataType) {
		int l = 0, r = page.size() - 1;
		int m;

		while (l <= r) {
			m = l + (r - l) / 2;
			ArrayList<String> a = (ArrayList<String>) page.get(m);
			// Check if x is present at mid
			if (compare(a.get(clusteringKeyIndex), val, clusterKeyType) == 0)
				return new int[] { m, r };

			// If x greater, ignore left half
			if (compare(a.get(clusteringKeyIndex), val, clusterKeyType) < 0)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}
//
		// if we reach here, then element was
		// not present
		return new int[] { -1, r };

	}

	public static void main(String[] args) {

//		System.out.println(compare("zzzzzz", "aaaaaa", "sdfs"));

		System.out.println(test());

	}

}
