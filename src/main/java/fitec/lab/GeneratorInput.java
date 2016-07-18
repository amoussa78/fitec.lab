package fitec.lab;

import java.util.Random;

public class GeneratorInput {

	public static void main(String args) {
		String words[] = {"iphone", "non", "musique", "oui", "mur", "chemise", "toit", "voiture", "maison", "camion",
						  "je", "chiffre", "test", "livre", "jambon", "plage", "vacance"};

		for (int i = 0; i < 100; ++i) {
			Random random = new Random();
			int h = random.nextInt(23 - 0 + 1) + 0;
			int min = random.nextInt(59 - 0 + 1) + 0;
			int s = random.nextInt(59 - 0 + 1) + 0;
			
			int a = random.nextInt(2016 - 2010 + 1) + 2010;
			int m = random.nextInt(12 - 1 + 1) + 1;
			int j = random.nextInt(28 - 1 + 1) + 1;
			System.out.println();
		}
	}
}
