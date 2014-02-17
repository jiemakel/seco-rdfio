package fi.seco.rdfio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotReader;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.lang.LangNTuple;
import org.apache.jena.riot.lang.LangRIOT;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.ParserProfileBase;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.riot.system.StreamRDFBase;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;

import fi.seco.openrdf.IllegalURICorrectingValueFactory;
import fi.seco.openrdf.RDFFormats;
import fi.seco.rdfobject.IQuad;
import fi.seco.rdfobject.IQuadVisitor;
import fi.seco.rdfobject.IRDFObject;
import fi.seco.rdfobject.ITriple;
import fi.seco.rdfobject.ITripleVisitor;
import fi.seco.rdfobject.URIResourceRDFObject;
import fi.seco.rdfobject.jena.JenaRDFObjectUtil;
import fi.seco.rdfobject.model.IRDFObjectQuadModel;
import fi.seco.rdfobject.model.IRDFObjectTripleModel;
import fi.seco.rdfobject.openrdf.OpenRDFRDFObjectUtil;

/**
 * An utility class for reading RDF. Uses efficient streaming parsers where
 * possible. Combines readers from Sesame and Jena with custom readers for the
 * Freebase dump format and Sindice DE tar format. Can read at least RDF/XML,
 * turtle, N3, n-triples, n-quads, trix, trig, rdf-json, binary, Freebase dump
 * and Sindice DE tar. Also supports gzip and bzip2 compressed sources.
 * 
 * @author jiemakel
 * 
 */
public class RDFReader {

	private static final Logger log = LoggerFactory.getLogger(RDFReader.class);

	static {
		RDFParserRegistry.getInstance(); // for initialization of formats;
	}

	/**
	 * A handler for streaming RDF quads and metadata
	 * 
	 * @author jiemakel
	 * 
	 */
	public interface IRDFHandler extends IQuadVisitor {
		/**
		 * A namespace definition encountered while processing RDF
		 * 
		 * @param prefix
		 *            the prefix for the namespace
		 * @param ns
		 *            the namespace for the prefix
		 */
		public void setNameSpace(String prefix, String ns);

		public void setBaseIRI(String baseIRI);

		/**
		 * A comment encountered while processing RDF
		 * 
		 * @param comment
		 *            the comment
		 */
		public void comment(String comment);

	}

	static RDFFormat getFormat(String url) {
		return RDFFormat.forFileName(url);
	}

	static Lang getLang(String url) {
		return RDFLanguages.filenameToLang(url);
	}

	static InputStream getInputStreamFromURL(String s) {
		try {
			if (s.endsWith(".xz")) return new XZCompressorInputStream(new URL(s).openStream());
			if (s.endsWith(".gz")) return new GZIPInputStream(new URL(s).openStream());
			if (s.endsWith(".bz2"))
				return new BZip2CompressorInputStream(new URL(s).openStream());
			else return new URL(s).openStream();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Check whether the format of the source url is a quad format, as
	 * determined by the extension
	 * 
	 * @param url
	 *            the file to check
	 * @return <code>true</code> if the url's extension indicates a quad format
	 */
	public static boolean isQuads(String url) {
		RDFFormat f = getFormat(url);
		if (f != null) return f.supportsContexts();
		return false;
	}

	/**
	 * Check whether the format of the source url is something that this reader
	 * can parse, as determined by the extension.
	 * 
	 * @param url
	 *            the file to check
	 * @return <code>true</code> if this reader can parse the file format
	 */
	public static boolean canRead(String url) {
		if (getFormat(url) != null) return true;
		return false;
	}

	/**
	 * Parse a file, streaming quads and metadata to an RDF handler
	 * 
	 * @param url
	 *            the location of the file to read
	 * @param handler
	 *            the RDF handler to pass quads and metadata to
	 * @throws IOException
	 * @throws RDFHandlerException
	 * @throws RDFParseException
	 */
	public static void read(String url, final IRDFHandler handler) throws IOException, RDFParseException, RDFHandlerException {
		read(getInputStreamFromURL(url), getFormat(url), new URIResourceRDFObject(url), url, handler);
	}

	public static void read(InputStream is, RDFFormat type, final IRDFObject dg, String baseURI,
			final IRDFHandler handler) throws IOException, RDFParseException, RDFHandlerException {
		Lang lang = RDFLanguages.nameToLang(type.getName());
		if (lang != null && !RDFLanguages.RDFXML.equals(lang)) {// openrdf parsers throw a fit if ttl lname starts with a number. RIOT seems faster also on at least NTRIPLES. But RIOT's RDF/XML parser is too strict
			LangRIOT parser = RiotReader.createParser(is, lang, baseURI, new StreamRDFBase() {

				@Override
				public void triple(Triple t) {
					handler.visit(new fi.seco.rdfobject.Quad(JenaRDFObjectUtil.getRDFObjectForNode(t.getSubject()), JenaRDFObjectUtil.getRDFObjectForNode(t.getPredicate()), JenaRDFObjectUtil.getRDFObjectForNode(t.getObject()), dg));
				}

				@Override
				public void prefix(String prefix, String iri) {
					handler.setNameSpace(prefix, iri);
				}

				@Override
				public void base(String iri) {
					handler.setBaseIRI(iri);
				}

			});
			if (parser instanceof LangNTuple<?>) ((LangNTuple<?>) parser).setSkipOnBadTerm(true);
			parser.setProfile(new ParserProfileBase(parser.getProfile().getPrologue(), ErrorHandlerFactory.errorHandlerWarn, LabelToNode.createUseLabelAsGiven()));
			parser.getProfile().getPrologue().setPrefixMapping(new PrefixMapStd(parser.getProfile().getPrologue().getPrefixMap()) {
				@Override
				public void add(String prefix, String iriString) {
					super.add(prefix, iriString);
					handler.setNameSpace(prefix, iriString);
				}

				@Override
				public void add(String prefix, IRI iri) {
					super.add(prefix, iri);
					handler.setNameSpace(prefix, iri.toString());
				}

				@Override
				public void putAll(PrefixMap pmap) {
					super.putAll(pmap);
					for (Map.Entry<String, IRI> e : pmap.getMapping().entrySet())
						handler.setNameSpace(e.getKey(), e.getValue().toString());
				}
			});
			parser.parse();
		} else if (RDFFormats.SINDICE_DE_TAR.equals(type))
			SindiceDETarParser.parse(is, handler);
		else if (RDFFormats.FREEBASE_QUADS.equals(type))
			FreebaseParser.transformData(new BufferedReader(new InputStreamReader(is)), new ITripleVisitor() {

				@Override
				public void visit(ITriple t) {
					handler.visit(new fi.seco.rdfobject.Quad(t.getSubject(), t.getProperty(), t.getObject(), dg));
				}

			});
		else {
			RDFParser p = Rio.createParser(type);
			p.setStopAtFirstError(false);
			p.setVerifyData(false);
			p.setValueFactory(IllegalURICorrectingValueFactory.instance);
			p.setDatatypeHandling(DatatypeHandling.IGNORE);
			if (type.supportsContexts())
				p.setRDFHandler(new RDFHandlerBase() {
					@Override
					public void handleNamespace(String prefix, String uri) {
						if ("".equals(prefix))
							handler.setBaseIRI(uri);
						else handler.setNameSpace(prefix, uri);
					}

					@Override
					public void handleStatement(Statement st) {
						handler.visit(OpenRDFRDFObjectUtil.getQuadForStatement(st));
					}

					@Override
					public void handleComment(String comment) {
						handler.comment(comment);
					}
				});
			else p.setRDFHandler(new RDFHandlerBase() {
				@Override
				public void handleNamespace(String prefix, String uri) {
					if ("".equals(prefix))
						handler.setBaseIRI(uri);
					else handler.setNameSpace(prefix, uri);
				}

				@Override
				public void handleStatement(Statement st) {
					handler.visit(new fi.seco.rdfobject.Quad(OpenRDFRDFObjectUtil.getRDFObjectForValue(st.getSubject()), OpenRDFRDFObjectUtil.getRDFObjectForValue(st.getPredicate()), OpenRDFRDFObjectUtil.getRDFObjectForValue(st.getObject()), dg));
				}

				@Override
				public void handleComment(String comment) {
					handler.comment(comment);
				}
			});
			p.parse(new InputStreamReader(is, "UTF-8"), baseURI);
		}
	}

	public static IRDFHandler getInserter(final IRDFObjectQuadModel m) {
		return new IRDFHandler() {

			@Override
			public void visit(IQuad q) {
				m.addQuad(q);
			}

			@Override
			public void setNameSpace(String prefix, String ns) {
				m.getPrefixMap().put(prefix, ns);
			}

			@Override
			public void comment(String comment) {

			}

			@Override
			public void setBaseIRI(String baseIRI) {}

		};
	}

	public static IRDFHandler getInserter(final IRDFObjectTripleModel m) {
		return new IRDFHandler() {

			@Override
			public void visit(IQuad q) {
				m.addTriple(q);
			}

			@Override
			public void setNameSpace(String prefix, String ns) {
				m.getPrefixMap().put(prefix, ns);
			}

			@Override
			public void comment(String comment) {

			}

			@Override
			public void setBaseIRI(String baseIRI) {}

		};
	}

}
