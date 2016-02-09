import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

import {PureSolTechnologiesComponent} from './puresol-technologies.component';
import {SocialBarComponent} from './social-bar.component';

@Component({
	selector: 'footer',
	directives: [
	  ROUTER_DIRECTIVES,
	  PureSolTechnologiesComponent,
	  SocialBarComponent
	],
	template:
`<div class="row">
  <hr />
</div>
<div class="row">
  <div class="col-md-4">
    <puresol-technologies></puresol-technologies>
  </div>
  <div class="col-md-4">
    <social-bar></social-bar>
  </div>
  <div class="col-md-4">
    <a [routerLink]="['Imprint']">Imprint</a>
  </div>
</div>`
})
export class FooterComponent {
}
