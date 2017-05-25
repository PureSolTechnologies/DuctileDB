import {Component} from '@angular/core';

import {HighlighterComponent} from './commons/highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation-requirements',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation-requirements.html'
})
export class DocumentationRequirementsComponent {
}

