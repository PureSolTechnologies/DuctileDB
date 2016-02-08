import {Component} from 'angular2/core';

import {PureSolTechnologiesComponent} from './puresol-technologies.component';

@Component({
	selector: 'header',
	directives: [PureSolTechnologiesComponent],
	template:
`<div class="navbar">
  <div class="col-sm-6">
    <a class="navbar-brand" href="#"><span class="logo"><span class="ductiledb">DuctileDB</span></span></a>
  </div>
  <div class="col-sm-6" style="text-align:right;">
    <span style="font-size:16pt;"><puresol-technologies></puresol-technologies></span>
  </div>
</div>`  
})
export class HeaderComponent {
}
