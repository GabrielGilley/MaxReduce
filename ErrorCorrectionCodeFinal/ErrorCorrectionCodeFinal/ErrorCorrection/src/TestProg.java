import flanagan.analysis.*;
import flanagan.circuits.*;
import flanagan.complex.*;
import flanagan.control.*;
import flanagan.integration.*;
import flanagan.interpolation.*;
import flanagan.io.*;
import flanagan.math.*;
import flanagan.optics.*;
import flanagan.physchem.*;
import flanagan.physprop.*;
import flanagan.plot.*;
import flanagan.roots.*;
import flanagan.util.*;
public class TestProg {
 public static void main(String[] args) {
	 System.out.println("Arghya");
	 Outliers ol = new Outliers();
	 double[] listDoub = {0.0, 9.9, 10, 10.3, 20.4, 8.6, 12.5};
	 System.out.println(ol.lowerOutliersTeitjenMoore(listDoub, 2));
	 
	 
 }
}
