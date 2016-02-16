import {Component} from 'angular2/core';

@Component({
	selector: 'imprint',
	template:
`<div class="container">
  <div class="row">
    <h1>Imprint</h1>
  </div>
  <div class="row">
    <address>
      <b>PureSol Technologies</b><br/>
      Rick-Rainer Ludwig<br/>
      Zum Heiderand 21<br/>
      01328 Dresden<br/>
      Germany<br/>
      <br/>
      Phone: +49 (0)162 42 44 195<br/>
      e-Mail: contact@puresol-technologies.com<br/>
      Internet: http://www.puresol-technologies.com
    </address>
    <p>
      Responsable for content regarding § 6 MDStV, see above.
    </p>
  </div>
  <div class="row">
    <h1>Responsibility Notice</h1>
    <p>
      Even with careful control we do not take responsibility for the content of external web pages. Responsible for the content of external pages are the operators of these.
    </p>
  </div>
  <div class="row">
    <h1>Disclaimer</h1>
    <p>
      The provided information on our pages was checked carefully and will be updated regularily. But we do not warrant that all information is always correct at all times. We might extent, remove or change any information without further notice.
    </p>
  </div>
  <div class="row">
    <h1>Data Privacy Notice</h1>
    <p>
      The usage of our web pages is possible without any publication of your identity or private data. Regarding the usage of the web pages, the server might log some information which might make it possible to get private information like IP address, date and time and pages viewed. We will not analyse the data for personal analysises. We might use the data which was made anonymous for statisical analysis.
    </p>
  </div>
</div>`
})
export class ImprintComponent {
}
