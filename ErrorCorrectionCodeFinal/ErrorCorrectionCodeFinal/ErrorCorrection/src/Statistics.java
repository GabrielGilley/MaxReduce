import java.util.*;
class Statistics 
{
    double getMean(double[] data)
    {
        double sum = 0.0;
        int size = data.length;
        for(double a : data) {
            sum += a;
        }
        return sum/size;
    }

    double getVariance(double[] data)
    {
        double mean = getMean(data);
        double temp = 0;
        for(double a : data) {
            temp += (mean-a) * (mean-a);
        }
        int size = data.length;
        return temp/size;
    }

    double getStdDev(double[] data)
    {
        return Math.sqrt(getVariance(data));
    }

    public double getMedian(double[] data) 
    {
       Arrays.sort(data);
       int size = data.length;
       
       if (size % 2 == 0) 
       {
          return (data[(size / 2) - 1] + data[size / 2]) / 2.0;
       } 
       else 
       {
          return data[size / 2];
       }
    }
    
    public double getMax(double[] data) {
    	double max = data[0];
    	for (int i = 0; i < data.length; i++) {
    		if (max < data[i]) {
    			max = data[i];
    		}
    	}
    	return max;
    }
}