package fi.seco.rdfio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.LangNTriples;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.RiotLib;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;

import fi.seco.rdfobject.BNodeResourceRDFObject;
import fi.seco.rdfobject.IQuad;
import fi.seco.rdfobject.IQuadVisitor;
import fi.seco.rdfobject.IRDFObject;
import fi.seco.rdfobject.Quad;
import fi.seco.rdfobject.URIResourceRDFObject;
import fi.seco.rdfobject.jena.JenaRDFObjectUtil;

public class SindiceDETarParser {

	private static final Logger log = LoggerFactory.getLogger(SindiceDETarParser.class);

	public static void parse(InputStream in, final IQuadVisitor visitor) {
		try {
			TarArchiveInputStream ta = new TarArchiveInputStream(in);
			BufferedReader r = new BufferedReader(new InputStreamReader(ta));
			TarArchiveEntry t;
			while ((t = ta.getNextTarEntry()) != null) {
				if (!t.getName().endsWith("metadata"))
					throw new IllegalArgumentException("Corrupt archive: " + t.getName() + " is not metadata");
				String graphURI = r.readLine();
				final IRDFObject graph = new URIResourceRDFObject(graphURI);
				String subjectURI = r.readLine();
				final IRDFObject subject;
				if (subjectURI.startsWith("_:"))
					subject = new BNodeResourceRDFObject(subjectURI);
				else subject = new URIResourceRDFObject(subjectURI);
				t = ta.getNextTarEntry();
				if (!t.getName().endsWith("outgoing-triples.nt"))
					throw new IllegalArgumentException("Corrupt archive: " + t.getName() + " is not outgoing-triples.nt");
				if (ta.available() > 0) {
					StreamRDF sink1 = new StreamRDFBase() {

						@Override
						public void triple(Triple t) {
							visitor.visit(new Quad(subject, JenaRDFObjectUtil.getRDFObjectForNode(t.getPredicate()), JenaRDFObjectUtil.getRDFObjectForNode(t.getObject()), graph));
						}

					};
					LangNTriples parser = new LangNTriples(TokenizerFactory.makeTokenizerUTF8(new CloseShieldInputStream(ta)), RiotLib.profile(RDFLanguages.NTRIPLES, null, ErrorHandlerFactory.errorHandlerWarn), sink1);
					parser.setSkipOnBadTerm(true);
					parser.parse();
				}
				t = ta.getNextTarEntry();
				if (!t.getName().endsWith("incoming-triples.nt"))
					throw new IllegalArgumentException("Corrupt archive: " + t.getName() + " is not incoming-triples.nt");
				if (ta.available() > 0) {
					StreamRDF sink2 = new StreamRDFBase() {

						@Override
						public void triple(Triple t) {
							visitor.visit(new Quad(JenaRDFObjectUtil.getRDFObjectForNode(t.getSubject()), JenaRDFObjectUtil.getRDFObjectForNode(t.getPredicate()), subject, graph));
						}

					};
					LangNTriples parser = new LangNTriples(TokenizerFactory.makeTokenizerUTF8(new CloseShieldInputStream(ta)), RiotLib.profile(RDFLanguages.NTRIPLES, null, ErrorHandlerFactory.errorHandlerWarn), sink2);
					parser.parse();
				}
			}
			ta.close();
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) throws IOException {
		IQuadVisitor v = new IQuadVisitor() {

			@Override
			public void visit(IQuad q) {
				System.out.println(q);
			}

		};
		parse(new GzipCompressorInputStream(new FileInputStream(new File("/local/data/sindice/DE-00724.sdetar.gz"))), v);
	}
}
