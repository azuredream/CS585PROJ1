package dataparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Customer
{
    public int ID;
    public String Name;
    public int Age;
    public String Gender;
    public int CountryCode;
    public float Salary;
 };
 class Transaction
 {
     public int TransID;
     public int CustID;
     public float TransTotal;
     public int TransNumItems;
     public String TransNumDesc;
  };
public class Parser {
	public static int CUSTOMERNUMER=50000;
	public static int NAMELENBOT=10;
	public static int NAMELENUPP=20;
	public static String chardic="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
	public static int AGEBOT=10;
	public static int AGEUPP=70;
	public static List<String> GENDER=List.of("male","female");
	public static int COUNTRYCODERANGE=10;
	public static float SALARYBOT=100;
	public static float SALARYUPP=10000;
	public static int TransIDrange=5000000;
    // 拼接文件完整路径
	public static String custfilepath = "/Users/zixuan/Desktop/Customers";
	public static String transfilepath="/Users/zixuan//Desktop/Transactions";
    public static List<String> readFileByLines(String fileName) {  
    	List<String> rlist=new ArrayList();
        File file = new File(fileName);  
        BufferedReader reader = null;  
        try {  
            System.out.println("reading by line");  
            reader = new BufferedReader(new FileReader(file));  
            String tempString = null;  
            int line = 1;  
            // 一次读入一行，直到读入null为文件结束  
            while ((tempString = reader.readLine()) != null) {  
                // 显示行号  
                System.out.println("line " + line + ": " + tempString);  
                rlist.add(tempString);
                line++;  
            }  
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }
        return rlist;
    }  
    public static List<Customer> getcustdatal(){
    	List<String> custstringlist=new ArrayList();
    	custstringlist=readFileByLines(custfilepath);
		List<Customer> customdata=new ArrayList<>();
		for(int i=0;i<=CUSTOMERNUMER-1;i++) {
			
			String datafield[]=custstringlist.get(i).split(",");
			
			
			Customer c=new Customer();
			c.ID=Integer.parseInt(datafield[0]);
			c.Name=datafield[1];
			c.Age=Integer.parseInt(datafield[2]);
			c.Gender=datafield[3];
			c.CountryCode=Integer.parseInt(datafield[4]);
			c.Salary=Float.parseFloat(datafield[5]);
			customdata.add(c);
		}
		return customdata;
    }
    public static List<Transaction> gettransdatal(){
    	List<String> transtringlist=new ArrayList();
    	transtringlist=readFileByLines(transfilepath);
		List<Transaction> transdata=new ArrayList<>();
		for(int i=1;i<=TransIDrange;i++) {
			String datafield2[]=transtringlist.get(i).split(",");
			Transaction t=new Transaction();
			t.TransID=Integer.parseInt(datafield2[0]);
			t.CustID=Integer.parseInt(datafield2[1]);
			t.TransTotal=Integer.parseInt(datafield2[2]);
			t.TransNumItems=Integer.parseInt(datafield2[3]);
			t.TransNumDesc=datafield2[4];
			transdata.add(t);
		}
		return transdata;
    }
    
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Customer> customdata=getcustdatal();
		List<Transaction> transdata=gettransdatal();
		
	}
};