package fi.seco.rdfio;

import java.io.BufferedReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.seco.rdfobject.IRDFObject;
import fi.seco.rdfobject.ITriple;
import fi.seco.rdfobject.ITripleVisitor;
import fi.seco.rdfobject.LiteralRDFObject;
import fi.seco.rdfobject.Triple;
import fi.seco.rdfobject.URIResourceRDFObject;
import fi.seco.util.LocaleUtil;

public class FreebaseParser {

	private static final Logger log = LoggerFactory.getLogger(FreebaseParser.class);
	private static final CharSequence fieldSeparator = "\t";
	private static final String freebaseNsPrefix = "http://rdf.freebase.com/ns/";

	private static final String DEFAULT_LANG_REGEX = "/lang/";
	private static final String KEY_TYPE1_REGEX = "type.key.namespace";
	private static final String KEY_TYPE2_REGEX = "type.object.key";
	private static final String BAD_ISBN = "soft.isbn.";

	private static final String convertId(String id) {
		return id.replace("/", ".");
	}

	private static final boolean isItKeyTypeAssertion(String predicate) {

		/*
		 * for (String regex: keyRegexList) { if (regex.contains(predicate))
		 * return true; }
		 */

		if (KEY_TYPE1_REGEX.equals(predicate) || KEY_TYPE2_REGEX.equals(predicate)) return true;

		return false;
	}

	public static void transformData(BufferedReader r, ITripleVisitor visitor) {
		String line;
		try {
			while ((line = r.readLine()) != null)
				transformQuad(line, visitor);
		} catch (IOException e) {
			log.error("", e);
		}
	}

	private static void transformQuad(String assertion, ITripleVisitor visitor) {
		if (assertion == null) throw new NullPointerException();

		String[] splits = assertion.split(fieldSeparator.toString());
		if (splits.length < 3 || splits.length > 4) {
			if (log.isDebugEnabled()) log.debug("Malformed (" + splits.length + " tuples): " + assertion);
			return;
		}

		String predicate = convertId(splits[1].substring(1, splits[1].length()));
		IRDFObject trs = new URIResourceRDFObject(freebaseNsPrefix + convertId(splits[0].substring(1, splits[0].length())));
		IRDFObject trp = new URIResourceRDFObject(freebaseNsPrefix + predicate);
		IRDFObject tro;
		if (splits.length == 3)
			tro = new URIResourceRDFObject(freebaseNsPrefix + convertId(splits[2].substring(1, splits[2].length())));
		else {
			String to = splits[2];
			String val = splits[3];
			if (to.length() == 0)
				tro = new LiteralRDFObject(val);
			else if (isItKeyTypeAssertion(predicate)) {
				to = convertId(to.substring(1, splits[2].length()));
				if (to.startsWith(BAD_ISBN)) {
					val = to.substring(BAD_ISBN.length());
					to = "soft.isbn";
				}
				tro = new LiteralRDFObject(val, null, freebaseNsPrefix + to);
			} else if (to.contains(DEFAULT_LANG_REGEX)) {
				to = to.replace(DEFAULT_LANG_REGEX, "");
				tro = new LiteralRDFObject(val, LocaleUtil.parseLocaleString(to));
			} else {
				if (log.isDebugEnabled()) log.debug("Couldn't understand quad: " + assertion);
				return;
			}
		}
		visitor.visit(new Triple(trs, trp, tro));
	}

	public static void main(String[] args) throws Exception {
		ITripleVisitor v = new ITripleVisitor() {

			@Override
			public void visit(ITriple i) {
				System.out.println(i);

			}

		};
		FreebaseParser.transformQuad("/m/0p_47\t/film/actor/film\t/m/02vcwc8", v);
		FreebaseParser.transformQuad("/m/0p_47	/film/actor/film	/m/02vcwc8", v);
		FreebaseParser.transformQuad("/m/0p_47\t/people/person/height_meters\t\tC\\C (ã‚·ãƒ³ãƒ‡ãƒ¬ãƒ©\\ã‚³ãƒ³ãƒ—ãƒ¬ãƒƒã‚¯ã‚¹)", v);
		FreebaseParser.transformQuad("/m/01hf9dc\t/type/object/name\t\t\"Don`t Shoot Me I`m Only the Piano Player\"", v);
		FreebaseParser.transformQuad("/m/0p_47\t/type/object/name\t/lang/en\tSteve Martin", v);
		FreebaseParser.transformQuad("/m/0p_47\t/type/object/key\t/wikipedia/pt\tSteve_Martin", v);
		FreebaseParser.transformQuad("/m/0p_47\t/type/object/name\t/lang/en\tSteve'Martin", v);
		FreebaseParser.transformQuad("/m/0p_47\t/type/object/name\t/lang/en\tSteve\"Martin", v);
		FreebaseParser.transformQuad("/m/063q09g	/freebase/labs_project/publicized_date		2009-06-04", v);
		FreebaseParser.transformQuad("/m/083tc7f\t/type/object/key\t/soft/isbn/9780789303837\tbest", v);
		FreebaseParser.transformQuad("/m/083tkhx\t/type/object/key\t/soft/isbn\t9780023418105", v);
		FreebaseParser.transformQuad("/m/083tkkn\t/type/object/key\t/soft/isbn\t9780023996016", v);
		FreebaseParser.transformQuad("/m/083tl91\t/type/object/key\t/soft/isbn/9780033349147\tbest", v);
		FreebaseParser.transformQuad("/m/083tldc\t/type/object/key\t/soft/isbn\t9780043321300", v);
		FreebaseParser.transformQuad("/m/026jl_d\t/type/object/name\t/guid/9202a8c04000641f8000000004684bec\t\"2004-11-22\"", v);

	}
}