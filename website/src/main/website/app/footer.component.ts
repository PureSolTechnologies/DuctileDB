import {Component} from '@angular/core';
import {ROUTER_DIRECTIVES} from '@angular/router';

import {PureSolTechnologiesComponent} from './commons/puresol-technologies.component';
import {SocialBarComponent} from './commons/social-bar.component';

@Component({
	selector: 'footer',
	directives: [
	  ROUTER_DIRECTIVES,
	  PureSolTechnologiesComponent,
	  SocialBarComponent
	],
	template:
`<hr />
<div class="container">
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
  </div>
</div>`
})
export class FooterComponent {
}
