package util;

import java.io.File;

public class FileUtils {
    /**
     * 移动文件
     * @author sweep
     * @param fileName
     * @param startPath
     * @param endPath
     * @return
     */
    public static boolean move(String fileName,String startPath,String endPath){
        File startFile=new File(startPath+fileName);
        File endFile = new File(endPath+fileName);
        if (endFile.exists()){
            endFile.delete();
        }
        if (startFile.renameTo(endFile)){
            return true;
        }else{
            return false;
        }
    }

    /**
     * 删除文件
     * @param
     */
    public static void deleteFile(File file){
        if (file.exists()){
            file.delete();
        }
    }

}
