import {Component} from '@angular/core';

@Component({
	selector: 'contribute',
	template:
`<div class="container"><div class="row">
  <div class="col-md-12">
    <h1>Contribute</h1>
    <p>
      Any help from any side is appreciated. There are multiple ways to contribute to DuctileDB.
    </p>
  </div>
</div>
<div class="row">
  <div class="col-md-6">
    <h2>Fork and Develop DuctileDB Code</h2>
    <p>
      DuctileDB is hosted on <a href="https://github.com/PureSolTechnologies/DuctileDB" target="_blank">GitHub</a>. Anyone can fork DuctileDB there, develop code and create a pull request to get the code merged into official DuctileDB.
    </p>
    <h2>File Bug Reports or Feature Requests</h2>
    <p>
      If bugs and issues are found, bug reports can be filed in the offical <a href="https://bugs.puresol-technologies.net/projects/ductiledb" target="_blank">DuctileDB Bug Tracker</a>. Ideas for features can be filed there as well.
    </p>
  </div>
  <div class="col-md-6">
    <h2>Write Documentation</h2>
    <p>
      This website is also hosted in GitHub together with the DuctileDB source code. Documentation writers can work on the same repository as well.
    </p>
  </div>
</div></div>`
})
export class ContributeComponent {
}
