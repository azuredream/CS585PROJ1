package cs585;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
 

public class datagenor {
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
	
	
	public static void main(String[] args) {
		List<Customer> customdata=new ArrayList<>();
		for(int i=1;i<=CUSTOMERNUMER;i++) {
			Customer c=new Customer();
			c.ID=i;
			int namelen=(int) (NAMELENBOT+Math.random()*(NAMELENUPP-NAMELENBOT+1));
			char name[]=new char[namelen];
			for(int j=1;j<=namelen;j++) {
				name[j-1]=chardic.charAt((int)(Math.random()*63));
			}
			c.Name=new String(name);
			c.Age=AGEBOT+(int)(Math.random()*(AGEUPP-AGEBOT+1));
			c.Gender=GENDER.get((int)(Math.random()*2));
			c.CountryCode=1+(int)(Math.random()*COUNTRYCODERANGE);
			c.Salary=(float) (SALARYBOT+Math.random()*(SALARYUPP-SALARYBOT));
			customdata.add(c);
			
		}
		List<Transaction> transdata=new ArrayList<>();
		for(int i=1;i<=TransIDrange;i++) {
			Transaction t=new Transaction();
			t.TransID=i;
			t.CustID=1+(int)(Math.random()*50000);
			t.TransTotal=10+(int)(Math.random()*991);
			t.TransNumItems=1+(int)(Math.random()*10);
			int desclen=(int) (20+Math.random()*(50-20+1));
			char desc[]=new char[desclen];
			for(int j=1;j<=desclen;j++) {
				desc[j-1]=chardic.charAt((int)(Math.random()*63));
			}
			t.TransNumDesc=new String(desc);
			transdata.add(t);
		}
			Boolean flag=true;
	        // file path
	        String custfilepath = "/Users/zixuan/Desktop/Customers";
	        String transfilepath="/Users/zixuan//Desktop/Transactions";
	        
	        
//
//	        // write
	        try {
	            // new file
	            File custfile = new File(custfilepath);
	            File transfile=new File(transfilepath);
	            
	            
	            
	            if (custfile.exists()) { // del file if exist
	            	custfile.delete();
	            }
	            if (transfile.exists()) { // del file if exist
	            	transfile.delete();
	            }
	            custfile.createNewFile();
	            transfile.createNewFile();
	            FileOutputStream custout=new FileOutputStream(custfile);
	            String dataline=new String();
	            for(int i=1;i<=CUSTOMERNUMER;i++) {
	            	Customer c=customdata.get(i-1);
	            	dataline=dataline+Integer.toString(c.ID)+","
	            			+new String(c.Name)+","
	            			+Integer.toString(c.Age)+","
	            			+new String(c.Gender)+","
	            			+Integer.toString(c.CountryCode)+","
	            			+Float.toString(c.Salary)+"\n";
	            	custout.write(dataline.getBytes());
	            	dataline="";
	            	System.out.print("cust:"+Integer.toString(i)+"\n");
	            			
	            }
	            custout.close();
	            FileOutputStream transout=new FileOutputStream(transfile);
	            for(int i=1;i<=TransIDrange;i++) {
	            	Transaction t=transdata.get(i-1);
	            	dataline=dataline+Integer.toString(t.TransID)+","
	            			+Integer.toString(t.CustID)+","
	            			+Float.toString(t.TransTotal)+","
	            			+Integer.toString(t.TransNumItems)+","
	            			+t.TransNumDesc+"\n";
	            	transout.write(dataline.getBytes());
	            	dataline="";
	            	System.out.print("trans:"+Integer.toString(i)+"\n");
	            			
	            }
	            transout.close();
	            
	        } catch (Exception e) {
	            flag = false;
	            e.printStackTrace();
	        }

	        // ret success or not
	        //return flag;
	    }
	}
