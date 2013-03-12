/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import java.io.IOException;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.ExtractedLinkIndri;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.ExtractedLinkIndri")
@OutputClass(className = "org.lemurproject.galago.core.types.ExtractedLink")
public class ELItoEL extends StandardStep<ExtractedLinkIndri, ExtractedLink> {

  @Override
  public void process(ExtractedLinkIndri link) throws IOException {
    ExtractedLink ln = new ExtractedLink();
    ln.anchorText = link.anchorText;
    ln.destName = link.destName;
    ln.srcName = link.srcName;
    ln.destUrl = link.destUrl;
    ln.srcUrl = link.srcUrl;
    
    processor.process(ln);
  }
}
