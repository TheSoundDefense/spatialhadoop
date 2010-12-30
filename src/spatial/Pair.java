package spatial;

public class Pair<T1, T2> {
	public final T1 t1;
	public final T2 t2;

	public Pair(T1 t1, T2 t2) {
		super();
		this.t1 = t1;
		this.t2 = t2;
	}
	
	@Override
	public boolean equals(Object obj) {
		@SuppressWarnings("unchecked")
		Pair<T1, T2> x = (Pair<T1, T2>) obj;
		return this.t1.equals(x.t1) && this.t2.equals(x.t2);
	}
}
