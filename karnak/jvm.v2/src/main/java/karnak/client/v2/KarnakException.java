
package karnak.client.v2;

public class KarnakException extends Exception {

protected static final long serialVersionUID = 1L;

public KarnakException(String message) {
        super(message);
}

public KarnakException(Throwable cause) {
        super(cause);
}
}
