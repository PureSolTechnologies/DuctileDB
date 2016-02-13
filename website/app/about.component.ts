import {Component} from 'angular2/core';

import {PureSolTechnologiesComponent} from './puresol-technologies.component';
import {PurifinityComponent} from './purifinity.component';

@Component({
	selector: 'about',
	directives: [
	  PureSolTechnologiesComponent,
	  PurifinityComponent
	],
	template:
`<div class="container"><div class="row">
  <div class="col-md-12">
    <h1>About</h1>
    <p>
      DuctileDB was started by <puresol-technologies></puresol-technologies> as side project for <purifinity></purifinity>, because a large graph database was needed to keep a large amount of information for source code analysis. 
    </p>
  </div>
</div></div>`
})
export class AboutComponent {
}
