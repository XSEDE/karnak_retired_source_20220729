
package karnak.service.predict;

public class PredictException extends Exception {
    public PredictException() {
	super("failed to create prediction");
    }

    public PredictException(String message) {
	super(message);
    }

    public PredictException(String message, Throwable cause) {
	super(message,cause);
    }

    public PredictException(Throwable cause) {
	super(cause);
    }
}
