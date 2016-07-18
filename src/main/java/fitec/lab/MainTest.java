package fitec.lab;

public class MainTest {
	public static void main(String args[]) {
		String s = "oui+non";
		s = s.replace("+", "--");
		System.out.println(s);
		String[] tab = s.split("--");
		System.out.println(tab[0]);
		System.out.println(tab[1]);
	}
}
