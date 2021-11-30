package cn.ucloud.ufile.fs;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
public class UfileTest {
    public static void main(String[] args){
        Result result = JUnitCore.runClasses(UFileFileSystemTest.class);
        for(Failure failure: result.getFailures()){
            System.out.println(failure.toString());
        }
        System.out.println("result: "+result.wasSuccessful());
    }
}
