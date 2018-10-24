package cn.zrmmpy.dataClean.utils;

import java.util.Random;

public class MusicInfoUtil {

	public static int getCategory() {
		Random random=new Random();
		
		return random.nextInt(13)+1;	
	}
}
