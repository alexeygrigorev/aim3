package de.tuberlin.dima.aim3.assignment4;

public class SecretTestRunner {

	public static void main(String[] args) throws Exception {
		String testSetPath = Config.pathToSecretTestSet();
		String outputPath = Config.pathToSecretTestOutput();
		int smoothing = Config.getSmoothingParameter();
		Classification.run(testSetPath, outputPath, smoothing);
	}

}
